# Query Scheduler for decision trees

## Content

## Project structure
The project is organized as follow :
- src/main/scala/imdb : code used to load the data set and evaluate the running time of the queries
  - Main / Maintest :
  - Package : provides all the functions to load the imdb data set into case classes, and other utils functions (parallel, print, ...) 
  - QueryHandler : includes the implementation of a set of SQL queries into Spark
  - Runner : Class evaluating the running time of the given queries implemented previously, load the results if already computed
  - DecisionTree : Place to define the probabilities of each query to success, and construct from it the probabilities of the decision tree
  
- src/main/python : code used to do visualizations and the Query Scheduler  
  - schedule_optimizer : main of the optimizer process, can be called from src/main/scala/imdb or executed apart from it
  - cp_optimizer : implementation of the CP formulation 
  - splitter : code describing the split procedure for the CP formulation
  - ilp_optimizer : implementation of the MILP
  - utils : contains utils functions (print, load, ...)
  - runtime_analysis : visualization of the running time of each query w.r.t. the partitions/cores
  - cp_visualizer : visualizations of the results of the CP execution
  
- src/main/resources : location of the data set



## Installation
