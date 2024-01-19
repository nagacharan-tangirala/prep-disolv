import xml.etree.ElementTree as et

import argparse

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--inputxml", type=str, required=True)
    args.add_argument("--outputxml", type=str, required=True)

    input_file = args.parse_args().inputxml
    output_file = args.parse_args().outputxml

    print("Reading XML file ", input_file)
    tree = et.parse(input_file)
    root = tree.getroot()
    veh_id = 0
    for child in root:
        if child.tag == "vehicle":
            child.attrib["id"] = str(veh_id)
            veh_id += 1
    tree.write(output_file)
