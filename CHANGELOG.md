# CHANGE LOG

## 1.6.5

DataPath feature enhancements and changes:
* new `denormalize()` method that uses a `visible-columns` "context" or a heuristic function to denormalize a path expression.
* new `limit()` method as a chaining operation so that clients don't need to explicity `fetch()` results with limits.
* subtle change to `link()` method for how it determines whether an alias name has been "bound" to the path. Now, it more explicitly checks if an alias object is found in the path's bound table instances. This change also relaxes the alias naming constraint to allow for automatic conflict resolution by appending an incremental numeric disambiguator if the given alias name is already in use. This could result in some `link()` methods now succeeding where they would have failed in the past.

## 1.0.0
Feature release. 
* Refactored catalog configuration API.
* Support for aggregate functions and binning in DataPath API.
* Various bug fixes.
* Complete change list [here](https://github.com/informatics-isi-edu/deriva-py/compare/v0.9.0...v1.0.0). 

Change lists for previous releases can be found on the GitHub releases 
page [here](https://github.com/informatics-isi-edu/deriva-py/releases).