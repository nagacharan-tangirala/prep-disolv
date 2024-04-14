# Prep-Disolv

This repository does the pre-processing of the mobility traces to be readable by [Disolv](https://github.com/nagacharan-tangirala/disolv).

### Features

- Reads SUMO Floating Car Data (FCD) traces and converts them to Disolv readable format.
- Generates Activation timing files for all the devices.
- Positions Road-side Units (RSUs) at junctions.

### Note

- Requirement and tool specific extensions are possible. New source of mobility traces can be added using the SUMO case as an example.
- The RSU placement can be extended to other types of placement strategies or even to other types of infrastructure.
- One-time scripts are added to the directory `scripts` for later reference.