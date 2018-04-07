#pragma once
#include <map>
#include <vector>
#include "Mixins.hpp"
#include "Relation.hpp"
//---------------------------------------------------------------------------
class Histogram {
    public:
    /// The relation the histogram refers to.
    const Relation& relation;
    /// The selection the histogram referst to.
    const SelectInfo &selection;
    /// Sampling Ratio.
    const unsigned samplingRate;
    /// Number of buckets used.
    const unsigned bucketCount;
    /// Minimum, Maximum values of the `selection` column.
    uint64_t minValue, maxValue;
    /// Histogram map.
    /// TODO: Change this to a `vector<pair<uint64_t, uint64_t>>`.
    ///       No need for an ordered map...
    std::map<uint64_t, uint64_t> histMap;

    /// Builds an equi-width histogram with `numberOfBuckets`.
    void buildEquiWidth(void);
    /// Get an estimation of the number of values in the
    /// range `[lowerBound, upperBound)`.
    uint64_t getEstimatedKeys(optional<uint64_t> lowerBound,
                              optional<uint64_t> upperBound);

    /// Constructor.
    Histogram(const Relation& relation, const SelectInfo &selection,
              unsigned samplingRate, unsigned bucketCount) :
        relation(relation), selection(selection),
        samplingRate(samplingRate), bucketCount(bucketCount) { }

#ifndef NDEBUG
    /// Print histogram.
    void printHistogram();
#endif

};
//---------------------------------------------------------------------------
