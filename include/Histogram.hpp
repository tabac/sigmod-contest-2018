#include <vector>
#include <map>
#include "Relation.hpp"

class Histogram{
    public:
    Relation& r;
    unsigned colId;
    uint64_t budget;
    uint64_t domainMinimum;
    /// Sample of the initial relation based on which the histogram is constructed
    uint64_t* sample;
    /// For the histogram we consider a map with key the upper bound of the bucket and value the
    /// corresponding frequency. For example, for the entries (X,f1),(Y,f2), we consider that there are f1 keys in
    /// the interval [0-X) and f2 keys in [X-Y).
    ///
    /// Moreover, we respect the classic assumption that keys are uniformly distributed within each bucket
    std::map<uint64_t, uint64_t> histo;

    Histogram();
    /// Constructor of approximate histogram based on a relation, column ID
    Histogram(Relation& r, unsigned colId);
    /// Constructor of approximate histogram based on a relation, column ID and sampling budget
    Histogram(Relation& r, unsigned colId, uint64_t budget);
    /// Creates an equi-height histogram with `numberOfBuckets` buckets
    void createEquiHeight(int numberOfBuckets);
    /// Creates an equi-width histogram with `numberOfBuckets` buckets
    void createEquiWidth(int numberOfBuckets);
    /// Get the number of the estimated keys in the specified range
    uint64_t getEstimatedKeys(uint64_t lowerBound, uint64_t upperBound);
    /// Print histogram for debug purposes
    void printHistogram();
};