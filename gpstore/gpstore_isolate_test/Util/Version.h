#ifndef _UTIL_VERSION_H
#define _UTIL_VERSION_H


#include "Util.h"

using namespace std;

typedef set<pair<TYPE_ENTITY_LITERAL_ID, TYPE_ENTITY_LITERAL_ID>> VDataSet;
typedef set<tuple<TYPE_ENTITY_LITERAL_ID, TYPE_ENTITY_LITERAL_ID, unsigned long long>> VXDataSet;
typedef set<tuple<TYPE_ENTITY_LITERAL_ID, TYPE_ENTITY_LITERAL_ID, const unsigned* >> VXXDataSet;
typedef pair<TYPE_ENTITY_LITERAL_ID, TYPE_ENTITY_LITERAL_ID> VData;
typedef vector<pair<TYPE_ENTITY_LITERAL_ID, TYPE_ENTITY_LITERAL_ID>> VDataArray;
const int init_size = 16;

//class Version
//{
//public:
//    typedef std::set<T> VDataSetz;
//
//private:
//    TYPE_ENTITY_LITERAL_ID * data;
//    TYPE_TXN_ID begin_ts;
//    TYPE_TXN_ID end_ts;
//public:
//    Version();
//    Version(TYPE_TXN_ID _begin_ts, TYPE_TXN_ID _end_ts);
//    Version& operator=(const Version& V) = delete;
//
//    ~Version() { free(data); data = nullptr; };
//    void add(VData value);
//    void remove(VData value);
//    void batch_add(VDataArray& values);
//    void batch_remove(VDataArray& values);
//    void expand(int num);
//    void print_data() const;
//    //get and set
//    TYPE_TXN_ID get_begin_ts() const {return this->begin_ts;};
//    TYPE_TXN_ID get_end_ts() const{ return this->end_ts;};
//
//    void get_add_set(VDataSet& add_set) const ;
//    void get_del_set(VDataSet& del_set) const ;
//
//    void get_version(VDataSet& add_set, VDataSet& del_set) const ;
//    void set_begin_ts(TYPE_TXN_ID _begin_ts){ this->begin_ts = _begin_ts;};
//    void set_end_ts(TYPE_TXN_ID _end_ts){ this->end_ts = _end_ts;};
//}__attribute__ ((aligned (8)));

/*
 * Version only for Delta Transaction, Cover Transaction just need vList for Version number;
 */
template <class T>
class Version
{
public:

    typedef set<T> VDataSetz;
    typedef vector<T> VDataArrayz;
    typedef T VDataz;
private:


//    TYPE_ENTITY_LITERAL_ID * data;
    VDataSetz add_set, del_set;

	TYPE_TXN_ID begin_ts;
	TYPE_TXN_ID end_ts;
public:
	Version();
	Version(TYPE_TXN_ID _begin_ts, TYPE_TXN_ID _end_ts);
	Version& operator=(const Version& V) = delete;

	~Version() {};
	void add(VDataz value);
	void remove(VDataz value);
	void batch_add(VDataArrayz& values);
	void batch_remove(VDataArrayz& values);
	void print_data() const;
	//get and set
	TYPE_TXN_ID get_begin_ts() const {return this->begin_ts;};
	TYPE_TXN_ID get_end_ts() const{ return this->end_ts;};

	void get_add_set(VDataSetz& Add) const ;
	void get_del_set(VDataSetz& Del) const ;

	void get_version(VDataSetz& add, VDataSetz& del) const ;
    int get_number();
	void set_begin_ts(TYPE_TXN_ID _begin_ts){ this->begin_ts = _begin_ts;};
	void set_end_ts(TYPE_TXN_ID _end_ts){ this->end_ts = _end_ts;};
}__attribute__ ((aligned (8)));

template <class T>
Version<T>::Version()
{
    add_set.clear();
    del_set.clear();

    this->begin_ts = INVALID_TS;
    this->end_ts = INVALID_TS;
}

template <class T>
Version<T>::Version(TYPE_TXN_ID _begin_ts, TYPE_TXN_ID _end_ts)
{
    add_set.clear();
    del_set.clear();

    this->begin_ts = _begin_ts;
    this->end_ts = _end_ts;
}

// Version& Version::operator=(const Version& V)
// {
// 	this->begin_ts = V.get_begin_ts();
// 	this->end_ts = V.get_end_ts();
// 	return *this;
// }

template <class T>
void Version<T>::add(VDataz value)
{
    auto del_it = del_set.find(value);
    if(del_it == del_set.end()){
        add_set.insert(value);
    }
    else del_set.erase(del_it);
}

template <class T>
void Version<T>::remove(VDataz value)
{
    auto add_it = add_set.find(value);
    if(add_it == add_set.end()){
        del_set.insert(value);
    }
    else add_set.erase(add_it);
}

template <class T>
void Version<T>::batch_add(VDataArrayz& values)
{
    for(auto value: values)
        add(value);
}

template <class T>
void Version<T>::batch_remove(VDataArrayz& values)
{
    for(auto value: values)
        del(value);
}

template <class T>
void Version<T>::get_add_set(VDataSetz& Add) const
{
    for(auto add_it : add_set)
        Add.insert(add_it);
}

template <class T>
void Version<T>::get_del_set(VDataSetz& Del) const
{

    for(auto del_it : del_set)
        Del.insert(del_it);
}

template <class T>
void Version<T>::get_version(VDataSetz& Add, VDataSetz& Del) const
{
    //print_data();
    for(auto add_it : add_set)
    {
        auto del_it = Del.find(add_it);
        if(del_it == Del.end()){
            Add.insert(add_it);
        }
        else Del.erase(del_it);
    }

    for(auto del_it : del_set)
    {
        auto add_it = Add.find(del_it);
        if(add_it == Add.end()){
            Del.insert(del_it);
        }
        else Add.erase(add_it);
    }
}

template <class T>
int Version<T>::get_number() {
    return add_set.size() - del_set.size();
}

template <class T>
void Version<T>::print_data() const
{
    std::cout << "add data : ";
    for(auto add_it : add_set)
        std::cout << add_it << " ";
    std::cout << "\ndel data : ";
    for(auto del_it : del_set)
        std::cout << del_it << " ";
    cout << "\n";
}


#endif
