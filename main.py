#!/usr/bin/env python3
import requests
import time
import sys
import duckdb
import os
from typing import Optional, Dict, Any

def round_9(val):
    return round(val * 1000000000) / 1000000000

def get_current_solana_epoch() -> Optional[int]:
    try:
        response = requests.post(
            "https://api.mainnet-beta.solana.com",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getEpochInfo"
            },
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            current_epoch = data.get("result", {}).get("epoch")
            if current_epoch is not None:
                print(f"Current Solana epoch: {current_epoch}", file=sys.stderr)
                return current_epoch

    except requests.RequestException as e:
        print(f"Error getting current epoch from Solana RPC: {e}", file=sys.stderr)
    
    return None

def get_epoch(identity: str, epoch: int) -> Optional[Dict[Any, Any]]:
    try:
        response = requests.get(
            f"https://api.trillium.so/validator_rewards/{epoch}",
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        # Find the validator with matching identity
        for validator in data:
            if validator.get("identity_pubkey") == identity:
                commission_bps = (validator.get("commission", 0) or 0) * 100
                mev_commission_bps = validator.get("mev_commission", 0) or 0

                # Calculate revenue components (rounded to 9 decimals)
                rewards = validator.get("rewards", 0) or 0
                mev_to_validator = validator.get("mev_to_validator", 0) or 0
                total_inflation_reward = validator.get("total_inflation_reward", 0) or 0
                vote_cost = validator.get("vote_cost", 0) or 0
                
                block_rewards = round_9(rewards)
                mev_to_validator_rounded = round_9(mev_to_validator)
                inflation_rewards = round_9(total_inflation_reward * commission_bps / 10000)

                base_revenue = round_9(rewards + inflation_rewards)
                total_revenue = round_9(rewards + mev_to_validator + inflation_rewards)
                vote_cost_rounded = round_9(vote_cost)
                net_earnings = round_9(rewards + mev_to_validator + inflation_rewards - vote_cost)
                
                return {
                    "epoch": epoch,
                    "name": validator.get("name", ""),
                    "identity": validator.get("identity_pubkey", ""),
                    "activated_stake": validator.get("activated_stake", 0) or 0,
                    "block_rewards": block_rewards,
                    "mev_to_validator": mev_to_validator_rounded,
                    "inflation_rewards": inflation_rewards,
                    "base_revenue": base_revenue,
                    "total_revenue": total_revenue,
                    "vote_cost": vote_cost_rounded,
                    "net_earnings": net_earnings,
                    "leader_slots": validator.get("leader_slots", 0),
                    "skip_rate": validator.get("skip_rate", 0),
                    "votes_cast": validator.get("votes_cast", 0),
                    "stake_percentage": validator.get("stake_percentage", 0),
                    "commission_bps": commission_bps,
                    "mev_commission_bps": mev_commission_bps,
                }
        
        return None
        
    except requests.RequestException as e:
        print(f"Error fetching epoch {epoch}: {e}", file=sys.stderr)
        return None


def check_epoch_exists(conn: duckdb.DuckDBPyConnection, identity: str, epoch: int) -> bool:
    """Check if epoch data exists or is marked as missing."""
    try:
        result = conn.execute("""
            SELECT 1 FROM (
                SELECT epoch, identity FROM rewards 
                UNION 
                SELECT epoch, identity FROM missing_rewards
            ) WHERE identity = ? AND epoch = ? LIMIT 1
        """, [identity, epoch]).fetchone()
        return result is not None
    except Exception:
        return False


def create_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rewards (
            epoch BIGINT, 
            name VARCHAR, 
            identity VARCHAR, 
            activated_stake BIGINT, 
            block_rewards DOUBLE, 
            mev_to_validator DOUBLE, 
            inflation_rewards DOUBLE, 
            base_revenue DOUBLE, 
            total_revenue DOUBLE, 
            vote_cost DOUBLE, 
            net_earnings DOUBLE, 
            leader_slots BIGINT, 
            skip_rate DOUBLE, 
            votes_cast BIGINT, 
            stake_percentage DOUBLE, 
            commission_bps BIGINT, 
            mev_commission_bps BIGINT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS missing_rewards (
            epoch BIGINT,
            identity VARCHAR
        );
    """)


def main():
    (identity, ) = sys.argv[1:]
    # Earliest epoch we can get from Trilium API is 600 (checked 2025-08-01)
    start_epoch = 600
    end_epoch = get_current_solana_epoch()
    if not end_epoch:
        raise f"Unable to determine current solana epoch"

    # We cannot query currently ongoing epoch
    end_epoch -= 1
    
    conn = duckdb.connect("data.duckdb")
    
    try:
        create_tables(conn)
        
        new_records = []
        for epoch in range(start_epoch, end_epoch + 1):
            if check_epoch_exists(conn, identity, epoch):
                continue
            
            print(f">>> Fetching epoch {epoch}", file=sys.stderr)
            epoch_data = get_epoch(identity, epoch)
            
            if epoch_data:
                new_records.append(epoch_data)
            else:
                print(f">>> Epoch {epoch} does not have data for {identity}", file=sys.stderr)
                conn.execute(
                    "INSERT INTO missing_rewards (epoch, identity) VALUES (?, ?)",
                    [epoch, identity],
                )
            
            # Be nice to Trilium's API
            time.sleep(1)
        
        if new_records:
            conn.executemany("""
                INSERT INTO rewards VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    record['epoch'], record['name'], record['identity'], record['activated_stake'],
                    record['block_rewards'], record['mev_to_validator'], record['inflation_rewards'],
                    record['base_revenue'], record['total_revenue'], record['vote_cost'],
                    record['net_earnings'], record['leader_slots'], record['skip_rate'],
                    record['votes_cast'], record['stake_percentage'], record['commission_bps'],
                    record['mev_commission_bps']
                ) for record in new_records
            ])
        else:
            print(">>> No new records to insert", file=sys.stderr)

        print(f">>> Generating CSV to '{identity}.csv'", file=sys.stderr)
        # HACK: mix and match with string interpolation, otherwise `duckdb.duckdb.ParserException`
        conn.execute(f"""
            COPY (SELECT * FROM rewards where identity = ? ORDER BY epoch ASC)
            TO '{identity}.csv' (HEADER, DELIMITER ',')
        """, [identity])
            
    finally:
        conn.close()


if __name__ == "__main__":
    main()
