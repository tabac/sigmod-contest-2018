#pragma once
#include <fstream>
#include <vector>
#include "Relation.hpp"
//---------------------------------------------------------------------------
class Utils {
    public:
    /// Create a dummy relation
    static Relation createRelation(RelationId relId, uint64_t size, uint64_t numColumns);

    /// Store a relation in all formats
    static void storeRelation(std::ofstream& out,Relation& r,unsigned i);

    /// Checks if a vector contains an element
    template<typename T>
    static bool contains(const std::vector<T> &v, const T& x) {
        return std::find(v.begin(), v.end(), x) != v.end();
    }
};
//---------------------------------------------------------------------------
