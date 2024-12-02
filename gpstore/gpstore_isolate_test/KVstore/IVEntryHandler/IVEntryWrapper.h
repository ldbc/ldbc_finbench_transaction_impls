#ifndef IVENTRY_WRAPPER_H
#define IVENTRY_WRAPPER_H

#include <iostream>
#include <memory>
#include <algorithm>
#include <cstring>

/**
 * My implementation for IVEntry, to benefit access to the data
*/
struct IVEntryWrapper {
	/**
	 * the data is like this:
	 * meta data:
	 * [0] = number of (p,o,x)
	 * [1] = number of p
	 * [2..2+3*number_p]: [2+3*i] = pid, [2+3*i+1] = block offset of pid, [2+3*i+2] = xwidth(in unsigned, i.e. 4B)
	 * 
	 * each block:
	 * [2+3*number_p+offet_p, 2+3*number_p+offset_(p+1)] = [o1, x1_1, x1_2, o2, x2_1, x2_2, ...]
	*/
	unsigned* data = nullptr;
	unsigned length = 0; /* number of unsigned in data */

	IVEntryWrapper(unsigned* _data, unsigned _length) : data(_data), length(_length) {}

    unsigned GetLength() { return length; }
	unsigned& unsigned_num() { return data[0]; }
	unsigned& number_of_p() { return data[1]; }
	int get_p_i(unsigned p) {
		int l = 0, r = number_of_p();
		while (l + 1 < r) {
			int mid = (l + r) / 2;
			if (pid(mid) <= p) {
				l = mid;
			} else {
				r = mid;
			}
		}
		if (pid(l) == p) {
			return l;
		} else {
			return -1;
		}
	}

private:
	unsigned& access(unsigned i) {
		if (i >= length) {
			std::cerr << "error in access\n";
			exit(1);
		}
		return data[i];
	}

// #define DEBUG
#ifdef DEBUG
#define DATA(x) access(x)
#else
#define DATA(x) data[x]
#endif

	unsigned& pid(unsigned i) { return DATA(2+3*i); }
	unsigned& GetPBlockOffset(unsigned i) { return DATA(2+3*i+1); }
    unsigned& GetPblockxwidth(unsigned i) { return DATA(2+3*i+2); }
	unsigned GetPblockEndBound(unsigned i) { 
		return i == number_of_p() - 1 ? unsigned_num() : GetPBlockOffset(i+1);
	}


public:
    struct PBlock {
        unsigned i; /* ith p block*/
        IVEntryWrapper& iv_entry_wrapper;
        PBlock(unsigned _i, IVEntryWrapper& _iv_entry_wrapper) : i(_i), iv_entry_wrapper(_iv_entry_wrapper) {}
        unsigned& p() { 
            return iv_entry_wrapper.pid(i);
        }
        unsigned& xwidth() { 
            return iv_entry_wrapper.GetPblockxwidth(i);
        }
        unsigned& start() { 
            return iv_entry_wrapper.GetPBlockOffset(i);
        }
		unsigned* ptr() {
			return &iv_entry_wrapper.data[start()];
		}
        unsigned end() { 
            return iv_entry_wrapper.GetPblockEndBound(i); 
        }
        unsigned size() {
            return (end() - start()) / (xwidth() + 1);
        }
        unsigned& o(unsigned j) { 
            #ifdef DEBUG
            return iv_entry_wrapper.access(start()+j*(xwidth()+1));
            #else
            return iv_entry_wrapper.data[start()+j*(xwidth()+1)];
            #endif
        }

		int get_o_i(unsigned o_val) {
			int l = 0, r = size();
			while (l + 1 < r) {
				int mid = (l + r) / 2;
				if (o(mid) <= o_val) {
					l = mid;
				} else {
					r = mid;
				}
			}
			if (o(l) == o_val) {
				return l;
			} else {
				return -1;
			}
		}

		private:
        unsigned* x_ptr(unsigned j) { 
            #ifdef DEBUG
            return (unsigned*) &iv_entry_wrapper.access(start()+j*(xwidth()+1)+1);
            #else
            return (unsigned*) &iv_entry_wrapper.data[start()+j*(xwidth()+1)+1];
            #endif
        }

		public:
		void SetXs(unsigned j, const unsigned* x) {
			#ifdef DEBUG
			unsigned unsigned_off = start()+j*(xwidth()+1)+1;
			for (unsigned k = 0; k < xwidth(); ++k) {
				iv_entry_wrapper.access(unsigned_off+k) = x[k];
			}
			#else
			unsigned *x_st = x_ptr(j);
			memcpy(x_st, x, xwidth() * sizeof(unsigned));
			#endif
		}

		void SetX(unsigned j, unsigned x_id, unsigned x_val) {
			#ifdef DEBUG
			iv_entry_wrapper.access(start()+j*(xwidth()+1)+1+x_id) = x_val;
			#else
			x_ptr(j)[x_id] = x_val;
			#endif
		}

		void GetXs(unsigned j, unsigned* x) {
			#ifdef DEBUG
			unsigned unsigned_off = start()+j*(xwidth()+1)+1;
			for (unsigned k = 0; k < xwidth(); ++k) {
				x[k] = iv_entry_wrapper.access(unsigned_off+k);
			}
			#else
			unsigned *x_st = x_ptr(j);
			memcpy(x, x_st, xwidth() * sizeof(unsigned));
			#endif
		}
    };

    PBlock pblock(unsigned i) {
        return PBlock(i, *this);
    }

	/**
	 * check version
	*/
	// unsigned& access(unsigned i) {
	// 	if (i >= length) {
	// 		std::cerr << "error in access\n";
	// 		exit(1);
	// 	}
	// 	return data[i];
	// }
	// unsigned& number_of_p_o_pairs() { return access(0); }
	// unsigned& number_of_p() { return access(1); }
	// unsigned& pid(unsigned i) { return access(2+2*i); }
	// unsigned& block_offset_of_pid(unsigned i) { return access(2+2*i+1); }
	// unsigned block_end_bnd_of_pid(unsigned i) { 
	// 	return i == number_of_p() - 1 ? number_of_p_o_pairs() : block_offset_of_pid(i+1);
	// }
	// unsigned& o(unsigned p, unsigned j) { 
	// 	return access(2+2*number_of_p()+(block_offset_of_pid(p)+j)*width);
	// }
	// unsigned long long* x(unsigned p, unsigned j) { 
	// 	return reinterpret_cast<unsigned long long*>(&access(2+2*number_of_p()+(block_offset_of_pid(p)+j)*width+1)); 
	// }
	// unsigned& o(unsigned po_offset) {
	// 	return access(2+2*number_of_p()+po_offset*width);
	// }
	// unsigned long long* x(unsigned po_offset) {
	// 	return reinterpret_cast<unsigned long long*>(&access(2+2*number_of_p()+po_offset*width+1));
	// }

	/* output IVEntry, Convert X in long long */
	void Output() {
		std::cout << "number of unsigned: " << unsigned_num() << std::endl;
		std::cout << "number of p: " << number_of_p() << std::endl;
		for (unsigned i = 0; i < number_of_p(); ++i) {
            auto pblock_i = pblock(i);
            std::cout << "p: " << pblock_i.p() << std::endl;
            std::cout << "xwidth: " << pblock_i.xwidth() << std::endl;
            std::cout << "start: " << pblock_i.start() << std::endl;
            std::cout << "end: " << pblock_i.end() << std::endl;
            std::cout << "size: " << pblock_i.size() << std::endl;
            for (unsigned j = 0; j < pblock_i.size(); ++j) {
                std::cout << "o: " << pblock_i.o(j) << std::endl;
				unsigned num_long_long = pblock_i.xwidth() / 2;
				std::unique_ptr<long long[]> x(new long long[num_long_long]);
				pblock_i.GetXs(j, (unsigned*)x.get());
				std::cout << "x: ";
				// for (unsigned k = 0; k < pblock_i.xwidth(); ++k) {
				// 	std::cout << x[k] << " ";
				// }
				std::for_each(x.get(), x.get() + num_long_long, [](auto x){std::cout << x << ", ";});
				std::cout << std::endl;
            }
		}
		std::cout << std::endl;
	}
};

#endif // IVENTRY_WRAPPER_H