import geopandas as gpd
from shapely.geometry import Polygon
from sumolib import geomhelper
import sumolib

# read sumo network
scenario_name = "ingolstadt"
project_path = "/Users/charan/workspace/disolv/" + scenario_name + "/raw/"
sumo_file = project_path + scenario_name + ".net.xml"

sumoNet = sumolib.net.readNet(sumo_file, withInternal=True)
# read the required features from lanes e.g. lane width, allowed vehicles or speed limit etc.
laneId = []
laneShape = []
laneWidth = []
laneAllow = []
laneSpeed = []

if __name__ == "__main__":
    for edge in sumoNet.getEdges():
        for lane in edge.getLanes():
            if lane.getLength() > 1 or lane.getLength() == -1:  # exclude very short lanes/edges but include internal links with length of -1
                laneId.append(lane.getID())
                laneShape.append(lane.getShape())
                laneWidth.append(lane.getWidth())
                laneAllow.append(list(lane.getPermissions()))
                laneSpeed.append(edge.getSpeed())

    polyLaneShape = {}
    for i in range(len(laneId)):
        geom_list = []
        tmp_coord1 = []
        tmp_coord2 = []
        tmp_geom1 = geomhelper.move2side(laneShape[i], laneWidth[i] / 2)
        tmp_geom2 = geomhelper.move2side(laneShape[i], -laneWidth[i] / 2)
        for k in tmp_geom1:
            tmp_coord1.append(sumoNet.convertXY2LonLat(k[0], k[1]))
        for k in tmp_geom2:
            tmp_coord2.append(sumoNet.convertXY2LonLat(k[0], k[1]))
        geom_list.append(tmp_coord1)
        geom_list.append(tmp_coord2[::-1])
        polyLaneShape[laneId[i]] = Polygon([item for sublist in geom_list for item in sublist])

    # read junctions
    polyNodeShape = {}
    nodeId = []
    nodeType = []
    for node in sumoNet.getNodes():
        nodeId.append(node.getID())
        nodeType.append(node.getType())
    for i in range(len(nodeId)):
        if len(sumoNet.getNode(nodeId[i]).getShape()) > 2:
            tmp_coord1 = []
            geom_list = []
            tmp_geom1 = sumoNet.getNode(nodeId[i]).getShape()
            for k in tmp_geom1:
                tmp_coord1.append(sumoNet.convertXY2LonLat(k[0], k[1]))
            geom_list.append(tmp_coord1)
        polyNodeShape[laneId[i]] = Polygon([item for sublist in geom_list for item in sublist])

    # convert to geopadas DataFrame
    df_node = gpd.GeoDataFrame({'nodeId': nodeId, 'nodeType': nodeType, 'geometry': polyNodeShape.values()},
                               crs="EPSG:4326")
    df_lane = gpd.GeoDataFrame(
        {'laneId': laneId, 'laneWidth': laneWidth, 'maxSpeed': laneSpeed, 'geometry': polyLaneShape.values()},
        crs="EPSG:4326")

    df_node_json = df_node.to_json()
    df_lane_json = df_lane.to_json()

    with open(project_path + "nodes.geojson", 'w') as outfile:
        outfile.write(df_node_json)
    with open(project_path + "lanes.geojson", 'w') as outfile:
        outfile.write(df_lane_json)