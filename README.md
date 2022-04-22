# MBEI - Model Based Event Interpreters 
MBEI is an experimental software package written in Rust that allows the use of **models** of physical assets such as factories and their sensors to engineer software (in any language supporting gRPC) that **interprets events** arising from these sensors in terms of the current state of affairs in order to keep track of the things that are moving through the factory, maintaining a complete and accurate history of the state of affairs.

The goal of MBEI is to make it easier to engineer, scale and reuse such software by leveraging models of physical assets. MBEI works in a distributed way, and can scale to arbitrarily large use cases. It is designed to provide low latencies (~1ms) when interpreting events, making it suitable for near-real time applications. It is released under an Apache 2.0 license.  

## Conceptual overview
MBEI makes writing a material tracker a matter of writing modular event interpreters in any programming language that supports gRPC, together with queries that match these modules to patterns of equipment and instrumentation in a graph based representation of the physical asset. 

For instance, a module may be responsible for interpreting events from a crane that lifts materials as updates to the states of these materials, e.g. their positions. The module is further associated with a query that identifies a crane and its surroundings. 

At deploy time, the query corresponding to a modular interpreter in instantiated once for each mach of the query, and receives the appropriate event data. In the case of our crane module, the module is instantiated once for each crane in the production plant. We call the framework level software that is responsible for calling the module the _component_ that corresponds to the module. 

Components keep state in a distributed way, and keep embedded databases with exactly the data they need to perform interpretations. Using the graph structure and the queries, we are able to determine exactly what data should be passed between deployed modules, so that all the information needed to interpret a new event is available. 

In the case of our crane, it will be kept updated about what materials are near it at any time by other modules. When a crane event occurs, e.g. picking up something, the module will know what is at the location of the pickup at that time, and update the location of this item to be picked up by the crane, passing this message to exactly the other modules that need to know.   

Since our database is distributed, we run the risk of out of order updates, for instance if due to a network issue our crane receives the update on what was at the pickup location after the pickup occurs. In this case, we will automatically restore consistency by reprocessing any potentially affected event, retracting erroneous interpretations in a cascade if necessary. 

The components keep a complete record of all that has happened in the factory. Such a record is useful in materials tracking, which is done for compliance reasons, in quality improvement and to improve production flow. Having the opportunity to retract historical interpretations is also important to recover when sensor- or human error provides erroneous input. 

## Benchmarks / experiments
Detailed instructions for running benchmarks can be found [here](https://github.com/magbak/mbei/blob/main/gcp-perftest/GKE-PERFTEST.MD).