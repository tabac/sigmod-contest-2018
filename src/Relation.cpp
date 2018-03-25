#include <thread>
#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cassert>
#include <optional>
#include "Mixins.hpp"
#include "Relation.hpp"
#include "Parser.hpp"
#include "Index.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void Relation::execute(vector<thread> &)
{
    {
        // Should never be called otherwise.
        assert(this->isStatusFresh());

        // Sould have only one incoming edge.
        assert(this->inAdjList.size() == 1);
        // Sould have one or zero outgoing edges.
        // assert(this->outAdjList.size() < 2);

        if (this->outAdjList.size() == 1) {
            // Should not be processed yet.
            assert(this->outAdjList[0]->isStatusFresh());
        }
    }

    // Check that all parent nodes are processed.
    bool allInProcessed = true;
    vector<AbstractNode *>::iterator it;
    for (it = this->inAdjList.begin(); it != this->inAdjList.end(); ++it) {
        allInProcessed &= (*it)->isStatusProcessed();
    }

#ifndef NDEBUG
    DEBUGLN("Love that jocker." + this->label);
#endif

    // If so set status to `processed`.
    if (allInProcessed) {
        this->setStatus(processed);
    }
}
//---------------------------------------------------------------------------
optional<IteratorPair> Relation::getIdsIterator(const SelectInfo& selectInfo,
                                                const FilterInfo* filterInfo)
// Returns an `IteratorPair` over all the `DataNode`'s ids.
{
    {
        assert(selectInfo.relId == this->relId);

        assert(filterInfo == NULL || filterInfo->filterColumn.relId == this->relId);
    }

    if (filterInfo != NULL && USE_INDEXES) {
        SortedIndex *index = this->getIndex(selectInfo);

        if (index != NULL) {
            // We have an index for this `selectInfo`, use it.
            return index->getIdsIterator(selectInfo, filterInfo);
        } else if (this->MAX_INDEX_COUNT < indexes.size()) {
            // We don't have an index for this `selectInfo`.
            // In the adaptive version we should create one.

            /*
            this->createIndex(selectInfo);

            return this->getIdsIterator(selectInfo, filterInfo);
            */
        }
    }

    return nullopt;
}
//---------------------------------------------------------------------------
optional<IteratorPair> Relation::getValuesIterator(const SelectInfo& selectInfo,
                                                   const FilterInfo* filterInfo)
// Returns an `IteratorPair` over all the `DataNode`'s values
// of the column specified by `selectInfo`.
{
    // Returns an empty optional if `selectInfo` does not
    // refair to this relation.
    if (selectInfo.relId != this->relId) {
        return nullopt;
    } else {
        if (filterInfo != NULL && USE_INDEXES) {
            SortedIndex *index = this->getIndex(selectInfo);

            if (index != NULL) {
                // We have an index for this `selectInfo`, use it.
                return index->getValuesIterator(selectInfo, filterInfo);
            } else {
                // We don't have an index for this `selectInfo`.
                // In the adaptive version we should create one.

                /*
                this->createIndex(selectInfo);

                return this->getValuesIterator(selectInfo, filterInfo);
                */
            }
        }
    }

    vector<uint64_t>::iterator begin (this->columns[selectInfo.colId]);
    vector<uint64_t>::iterator end (this->columns[selectInfo.colId] + this->size);

    return optional<IteratorPair>{{begin, end}};
}
//---------------------------------------------------------------------------
SortedIndex* Relation::getIndex(const SelectInfo &selection)
{
    vector<SortedIndex>::iterator it;
    for (it = this->indexes.begin(); it != this->indexes.end(); ++it) {
        if (it->selection == selection) {
            return &(*it);
        }
    }

    return NULL;
}
//---------------------------------------------------------------------------
void Relation::createIndex(const SelectInfo &selection)
{
    vector<SelectInfo>::iterator it = find(this->columnsInfo.begin(),
                                           this->columnsInfo.end(),
                                           selection);

    assert(it != this->columnsInfo.end());

    vector<uint64_t>::iterator begin (this->columns[selection.colId]);
    vector<uint64_t>::iterator end (this->columns[selection.colId] + this->size);

    this->indexes.emplace_back((*it), make_pair(begin, end));
}
//---------------------------------------------------------------------------
void Relation::storeRelation(const string& fileName)
  // Stores a relation into a binary file
{
    ofstream outFile;
    outFile.open(fileName,ios::out|ios::binary);
    outFile.write((char*)&size,sizeof(size));
    auto numColumns=columns.size();
    outFile.write((char*)&numColumns,sizeof(size_t));
    for (auto c : columns) {
        outFile.write((char*)c,size*sizeof(uint64_t));
    }
    outFile.close();
}
//---------------------------------------------------------------------------
void Relation::storeRelationCSV(const string& fileName)
  // Stores a relation into a file (csv), e.g., for loading/testing it with a DBMS
{
    ofstream outFile;
    outFile.open(fileName+".tbl",ios::out);
    for (uint64_t i=0;i<size;++i) {
        for (auto& c : columns) {
            outFile << c[i] << '|';
        }
        outFile << "\n";
    }
}
//---------------------------------------------------------------------------
void Relation::dumpSQL(const string& fileName,unsigned relationId)
  // Dump SQL: Create and load table (PostgreSQL)
{
    ofstream outFile;
    outFile.open(fileName+".sql",ios::out);
    // Create table statement
    outFile << "CREATE TABLE r" << relationId << " (";
    for (unsigned cId=0;cId<columns.size();++cId) {
        outFile << "c" << cId << " bigint" << (cId<columns.size()-1?",":"");
    }
    outFile << ");\n";
    // Load from csv statement
    outFile << "copy r" << relationId << " from 'r" << relationId << ".tbl' delimiter '|';\n";
}
//---------------------------------------------------------------------------
void Relation::loadRelation(const char* fileName)
{
    int fd = open(fileName, O_RDONLY);
    if (fd==-1) {
        cerr << "cannot open " << fileName << endl;
        throw;
    }

    // Obtain file size
    struct stat sb;
    if (fstat(fd,&sb)==-1)
        cerr << "fstat\n";

    auto length=sb.st_size;

    char* addr=static_cast<char*>(mmap(nullptr,length,PROT_READ,MAP_PRIVATE,fd,0u));
    if (addr==MAP_FAILED) {
        cerr << "cannot mmap " << fileName << " of length " << length << endl;
        throw;
    }

    if (length<16) {
        cerr << "relation file " << fileName << " does not contain a valid header" << endl;
        throw;
    }

    this->size=*reinterpret_cast<uint64_t*>(addr);
    addr+=sizeof(size);
    auto numColumns=*reinterpret_cast<size_t*>(addr);
    addr+=sizeof(size_t);
    for (unsigned i=0;i<numColumns;++i) {
        this->columns.push_back(reinterpret_cast<uint64_t*>(addr));
        addr+=size*sizeof(uint64_t);
    }
}
//---------------------------------------------------------------------------
Relation::Relation(RelationId relId, const char* fileName) : ownsMemory(false), relId(relId)
// Constructor that loads relation from disk
{
    loadRelation(fileName);

    // Reserve memory for column names.
    this->columnsInfo.reserve(this->columns.size());
    // Create relations column `SelectInfo` objects.
    for (unsigned c = 0; c < this->columns.size(); ++c) {
        this->columnsInfo.emplace_back(relId, 0, c);
    }

#ifndef NDEBUG
    this->label = "r" + to_string(this->relId);
#endif

}
//---------------------------------------------------------------------------
Relation::~Relation()
  // Destructor
{
    if (ownsMemory) {
        for (auto c : columns)
            delete[] c;
    }
}
//---------------------------------------------------------------------------
