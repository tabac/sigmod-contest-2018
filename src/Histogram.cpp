#include <map>
#include <limits>
#include <vector>
#include <random>
#include <cstdlib>
#include <iostream>
#include "Histogram.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void Histogram::buildEquiWidth(void) {
    // Values of the histogram's column.
    const uint64_t *values = relation.columns[selection.colId];

    // Find min, max values for column.
    uint64_t maxValueLoc = 0;
    uint64_t minValueLoc = numeric_limits<uint64_t>::max();
    for (size_t i = 0; i < relation.size; ++i) {
        if (values[i] > maxValueLoc) {
            maxValueLoc = values[i];
        }
        if (values[i] < minValueLoc) {
            minValueLoc = values[i];
        }
    }
    this->minValue = minValueLoc;
    this->maxValue = maxValueLoc;


    // Initialize random generator.
    default_random_engine generator;
    uniform_int_distribution<size_t> distribution(0, samplingRate);

    // Calculate bucket's size.
    size_t bucketSize = (maxValueLoc - minValueLoc) / bucketCount + 1;

    // Sample values and update buckets.
    for (size_t i = 0; i < relation.size; i += samplingRate) {
        size_t offset = distribution(generator);

        if (i + offset < relation.size) {
            uint64_t valueBucketUpperBound = (
                (values[i + offset] / bucketSize) * bucketSize + bucketSize);

            histMap[valueBucketUpperBound] += samplingRate;
        }
    }

    assert(histMap.size() <= bucketCount);

#ifndef NDEBUG
    printHistogram();
#endif

}
//---------------------------------------------------------------------------
uint64_t Histogram::getEstimatedKeys(optional<uint64_t> lowerBound,
                                     optional<uint64_t> upperBound) const
{
}
//---------------------------------------------------------------------------
void Histogram::mergeHistograms(const Histogram *left, const Histogram *right,
                                Histogram &output)
{
    if (left->minValue > right->minValue) {
        swap(left, right);
    }

    uint64_t minValueLoc = max(left->minValue, right->minValue);
    uint64_t maxValueLoc = min(left->maxValue, right->maxValue);

    uint64_t bucketCountLoc = max(left->bucketCount, right->bucketCount);

    size_t bucketSize = (maxValueLoc - minValueLoc) / bucketCountLoc + 1;
    if (bucketSize < 10) {
        bucketSize = 10;

        bucketCountLoc = (maxValueLoc - minValueLoc) / bucketSize + 1;
    }

    uint64_t lowerBound = minValueLoc;
    for (size_t i = 0; i < bucketCountLoc; ++i) {
        uint64_t leftKeys = left->getEstimatedKeys(lowerBound,
                                                   lowerBound + bucketSize);
        uint64_t rightKeys = right->getEstimatedKeys(lowerBound,
                                                     lowerBound + bucketSize);
        lowerBound += bucketSize;

        output.histMap[lowerBound] = (leftKeys * rightKeys) / 2;
    }

    output.minValue = minValueLoc;
    output.maxValue = maxValueLoc;
    output.bucketCount = bucketCountLoc;

    assert(output.histMap.size() <= bucketCountLoc);
}
//---------------------------------------------------------------------------
#ifndef NDEBUG
void Histogram::printHistogram() {
    cerr << "Histogram of: "   << this->selection.dumpLabel()
         << " (samplingRate: " << this->samplingRate
         << ", bucketCount: "  << this->bucketCount
         << ", minValue: "     << this->minValue
         << ", maxValue: "     << this->maxValue << ")" << endl;

    map<uint64_t, uint64_t>::iterator it;
    for (it = this->histMap.begin(); it != this->histMap.end(); ++it) {
        cerr << it->first << "-->" << it->second << endl;
    }
}
#endif
//---------------------------------------------------------------------------
