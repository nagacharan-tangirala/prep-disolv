import argparse
from pathlib import Path

import pandas as pd


def prepare_veh_id_map(veh_actions: pd.DataFrame) -> dict[str, str]:
    agent_ids = veh_actions["agent_id"]
    ns3_ids = range(0, len(agent_ids))
    id_map = dict(zip(agent_ids, ns3_ids))
    return id_map


def prepare_rsu_id_map(rsu_actions: pd.DataFrame, veh_count: int) -> dict[str, str]:
    agent_ids = rsu_actions["agent_id"]
    ns3_ids = range(veh_count, len(agent_ids) + veh_count)
    id_map = dict(zip(agent_ids, ns3_ids))
    return id_map


def modify_agent_ids(input_df: pd.DataFrame, veh_id_map: dict[str, str]) -> pd.DataFrame:
    new_df = input_df.copy()
    new_df["agent_id"] = new_df["agent_id"].map(veh_id_map)
    return new_df


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--scenario", type=str, required=True)
    scenario = args.parse_args().scenario

    input_path = Path("/Users/charan/workspace/disolv/input/" + scenario)
    output_path = Path("/Users/charan/workspace/disolv/input/" + scenario + "_ns3")

    # Create directories
    output_path.mkdir(exist_ok=True)
    act_path = output_path / "activations"
    act_path.mkdir(exist_ok=True)
    links_path = output_path / "links"
    links_path.mkdir(exist_ok=True)
    pos_path = output_path / "positions"
    pos_path.mkdir(exist_ok=True)

    # Read vehicle activations and create veh ID map
    veh_activations = input_path / "activations" / "vehicle_activations.parquet"
    veh_ac_df = pd.read_parquet(veh_activations)
    veh_id_map = prepare_veh_id_map(veh_ac_df)

    # Read RSU activations and create RSU id map
    rsu_activations = input_path / "activations" / "rsu_activations.parquet"
    rsu_ac_df = pd.read_parquet(rsu_activations)
    rsu_id_map = prepare_rsu_id_map(rsu_ac_df, len(veh_id_map))

    modified_df = modify_agent_ids(veh_ac_df, veh_id_map)
    new_veh_activations = output_path / "activations" / "vehicle_activations.parquet"
    modified_df.to_parquet(new_veh_activations)

    modified_df = modify_agent_ids(rsu_ac_df, rsu_id_map)
    new_rsu_activations = output_path / "activations" / "rsu_activations.parquet"
    modified_df.to_parquet(new_rsu_activations)

    # Modify the links files
    links_file = input_path / "links" / "v2r_links.parquet"
    links_df = pd.read_parquet(links_file)
    links_df["agent_id"] = links_df["agent_id"].map(veh_id_map)
    links_df["target_id"] = links_df["target_id"].map(rsu_id_map)
    new_links = output_path / "links" / "v2r_links.parquet"
    links_df.to_parquet(new_links)

    links_file = input_path / "links" / "v2v_links.parquet"
    links_df = pd.read_parquet(links_file)
    links_df["agent_id"] = links_df["agent_id"].map(veh_id_map)
    links_df["target_id"] = links_df["target_id"].map(veh_id_map)
    new_links = output_path / "links" / "v2v_links.parquet"
    modified_df.to_parquet(new_links)

    links_file = input_path / "links" / "r2v_links.parquet"
    links_df = pd.read_parquet(links_file)
    links_df["agent_id"] = links_df["agent_id"].map(rsu_id_map)
    links_df["target_id"] = links_df["target_id"].map(veh_id_map)
    new_links = output_path / "links" / "r2v_links.parquet"
    modified_df.to_parquet(new_links)

    links_file = input_path / "links" / "r2r_links.parquet"
    links_df = pd.read_parquet(links_file)
    links_df["agent_id"] = links_df["agent_id"].map(rsu_id_map)
    links_df["target_id"] = links_df["target_id"].map(rsu_id_map)
    new_links = output_path / "links" / "r2r_links.parquet"
    modified_df.to_parquet(new_links)

    veh_positions = input_path / "positions" / (scenario + "_fcd.parquet")
    veh_positions_df = pd.read_parquet(veh_positions)
    modified_df = modify_agent_ids(veh_positions_df, veh_id_map)
    new_positions = output_path / "positions" / (scenario + "_fcd.parquet")
    modified_df.to_parquet(new_positions)

    rsu_positions = input_path / "positions" / "rsu_deployment.parquet"
    rsu_positions_df = pd.read_parquet(rsu_positions)
    modified_df = modify_agent_ids(rsu_positions_df, rsu_id_map)
    new_positions = output_path / "positions" / "rsu_deployment.parquet"
    modified_df.to_parquet(new_positions)

    # Save the ID mappings to the output.
