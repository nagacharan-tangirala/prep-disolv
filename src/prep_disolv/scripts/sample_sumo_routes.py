import argparse
import random
import xml.etree.ElementTree as Et
from pathlib import Path

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--scenario", type=str, required=True)
    args.add_argument("--factor", type=float, required=True)
    scenario = args.parse_args().scenario
    factor = args.parse_args().factor

    input_path = Path("/mnt/hdd/workspace/disolv/raw/" + scenario)
    sumo_file = input_path / (scenario + ".rou.xml")
    output_file = input_path / (scenario + "_{}_scaled.rou.xml".format(factor))
    print("Reading sumo file from", sumo_file)

    tree = Et.parse(sumo_file)
    root = tree.getroot()

    # Get vehicle ID in 10 second intervals
    vehicle_ids = {}
    vehicle_count = {}
    for vehicle in root.findall("vehicle"):
        depart = float(vehicle.get("depart"))
        depart = int(depart / 10) * 10
        if depart not in vehicle_ids:
            vehicle_ids[depart] = []
            vehicle_count[depart] = 0
        vehicle_ids[depart].append(vehicle.get("id"))
        vehicle_count[depart] += 1

    # Scale down the number of vehicles
    for vehicle in root.findall("vehicle"):
        depart = float(vehicle.get("depart"))
        depart = int(depart / 10) * 10
        # Get the vehicle count at this time step
        count = vehicle_count[depart]
        # Get scaled down count
        scaled_count = int(count * factor)
        # Sample scaled_count vehicle IDs
        sampled_vehicles = random.sample(vehicle_ids[depart], scaled_count)
        # Remove other vehicles from the tree
        if vehicle.get("id") not in sampled_vehicles:
            root.remove(vehicle)

    # Reset vehicle IDs in the tree
    for i, vehicle in enumerate(root.findall("vehicle")):
        vehicle.set("id", "{}".format(i))

    # Write the new file
    tree.write(output_file)
