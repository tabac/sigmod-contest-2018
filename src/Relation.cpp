#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cassert>
#include "Mixins.hpp"
#include "Relation.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
IteratorPair Relation::getIdsIterator(FilterInfo* filterInfo)
// Returns an `IteratorPair` over all the `DataNode`'s ids.
{
    assert(filterInfo == NULL);

    return {this->ids.begin(), this->ids.end()};
}
//---------------------------------------------------------------------------
IteratorPair Relation::getValuesIterator(SelectInfo& selectInfo, FilterInfo* filterInfo)
// Returns an `IteratorPair` over all the `DataNode`'s values
// of the column specified by `selectInfo`.
{
    assert(filterInfo == NULL);

    assert(selectInfo.relId == this->relId);

    vector<uint64_t>::iterator begin (this->columns[selectInfo.colId]);
    vector<uint64_t>::iterator end (this->columns[selectInfo.colId] + this->size);

    return {begin, end};
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

    // Reserve memory for ids.
    this->ids.reserve(this->size);

    // Store ids to `ids` vector.
    for (uint64_t i = 0; i < this->size; ++i) {
        this->ids.push_back(i);
    }
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
