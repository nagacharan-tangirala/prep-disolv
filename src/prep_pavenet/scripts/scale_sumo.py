# This script takes scales up the sumo routes by a given factor.

import argparse
import xml.etree.ElementTree as Et
import copy


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--input", type=str, required=True)
    args.add_argument("--factor", type=int, required=True)
    args.add_argument("--buffer", type=float, required=True)
    args.add_argument("--output", type=str, required=True)

    input_file = args.parse_args().input
    output_file = args.parse_args().output
    factor = args.parse_args().factor
    buffer = args.parse_args().buffer

    # Read the sumo route file xml
    tree = Et.parse(input_file)
    root = tree.getroot()
    root_attrib = root.attrib

    # Read the routes
    routes = root.findall("route")
    vehicles = root.findall("vehicle")
    vtype = root.findall("vType")

    # Scale the routes
    new_routes = []
    new_vehicles = []
    route_id = 0
    vehicle_template = vehicles[0]
    for i in range(factor):
        for route in routes:
            new_route = copy.deepcopy(route)
            new_route.attrib["id"] = f"{route_id}"
            new_routes.append(new_route)
            vehicle = copy.deepcopy(vehicle_template)
            vehicle.attrib["id"] = f"{route_id}"
            vehicle.attrib["route"] = f"{route_id}"
            vehicle.attrib["depart"] = f"{buffer * i}"
            new_vehicles.append(vehicle)
            route_id += 1

    # Write the new routes to the output file
    root.clear()
    root.attrib = root_attrib
    root.extend(vtype)
    root.extend(new_routes)
    root.extend(new_vehicles)
    tree.write(output_file, encoding="utf-8", short_empty_elements=True)
