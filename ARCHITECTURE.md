# Architecture

This document describes the high level architecture of the nwp-consumer project.

## Birds-eye view

```mermaid
flowchart
    subgraph "Hexagonal Architecture"

        subgraph "NWP Consumer"
            subgraph "Ports"
                portFI(FetcherInterface) --- core
                core --- portSI(StorageInterface)

                subgraph "Core"                    
                    core{{Domain Logic}}
                end
            end
        end

        subgraph "Driving Adaptors"
            i1{ICON} --implements--> portFI
            i2{ECMWF} --implements--> portFI
            i3{MetOffice} --implements--> portFI
        end

        subgraph "Driven Adaptors"
            portSI --- o1{S3}
            portSI --- o2{Huggingface}
            portSI --- o3{LocalFS}
        end
        
    end
```

At the top level, the consumer downloads raw NWP data, processes it to zarr, and saves it to a storage backend.

It is built following the hexagonal architecture pattern.
This pattern is used to separate the core business logic from the driving and driven adaptors.
The core business logic is the `service` module, which contains the domain logic.
This logic is agnostic to the driving and driven actors,
instead relying on abstract classes as the ports to interact with them.


## Entry Points

`src/nwp_consumer/cmd/main.py` contains the main function which runs the consumer.

`src/nwp_consumer/internal/service/consumer.py` contains the `NWPConsumer` class,
the methods of which are the business use cases of the consumer.

`StorageInterface` and `FetcherInterface` classes define the ports used by driving and driven actors.

`src/nwp_consumer/internal/inputs` contains the adaptors for the driving actors.

`src/nwp_consumer/internal/outputs` contains the adaptors for the driven actors.

## Core

The core business logic is contained in the `service` module.
According to the hexagonal pattern, the core logic is agnostic to the driving and driven actors.
As such, there is an internal data representation of the NWP data that the core logic acts upon.
Due to the multidimensional data of the NWP data, it is hard to define a schema for this.

Internal data is stored an xarray dataset.
This dataset effectively acts as an array of `DataArrays` for each parameter or variable.
It should have the following dimensions and coordinates:

- `time` dimension
- `step` dimension
- `latitude` or `x` dimension
- `longitude` or `y` dimension

Parameters should be stored as DataArrays in the dataset.