#ifndef IV_ENTRY_HANDLER_H
#define IV_ENTRY_HANDLER_H

#include <set>
#include <tuple>

#include "IVEntryWrapper.h"


typedef unsigned TYPE_ENTITY_LITERAL_ID;

typedef std::set<std::tuple<TYPE_ENTITY_LITERAL_ID, TYPE_ENTITY_LITERAL_ID,
                            const unsigned *>>
    VXXDataSet;

namespace IVEntryHandler {
extern unsigned merge_pox_cnt;
/**
 * @note xwidth is the number of unsigned in xlist
*/
void InsertPOXs(const VXXDataSet &_pidsoidlist, unsigned xwidth, unsigned *_tmp,
                unsigned long _len, unsigned *&values,
                unsigned long &values_len);
/**
 * @note xwidth is the number of long long in xlist
*/
void Getoxlist(IVEntryWrapper &entry, unsigned p, unsigned *&olist,
               long long *&xlist, unsigned &len, unsigned &xwidth_in_long_long);
}  // namespace IVEntryHandler

#endif  // IV_ENTRY_HANDLER_H