#ifndef _UTIL_LATCH_H
#define _UTIL_LATCH_H

#include <pthread.h>
#include <iostream>
class Latch
{
private:
	/// The real latch
	pthread_rwlock_t lock;


public:
	Latch(const Latch&);
	void operator=(const Latch&);
	/// Constructor
	Latch();
	/// Destructor
	~Latch();

	/// Lock exclusive
	void lockExclusive();
	/// Try to lock exclusive
	bool tryLockExclusive();
	/// Lock shared
	void lockShared();
	/// Try to lock shared
	bool tryLockShared();
	/// Release the lock. Returns true if the lock seems to be free now (hint only)
	bool unlock();
};

#endif
