#include "Pipe.h"
#include <stdlib.h>
#include <iostream> 

Pipe :: Pipe (int bufferSize) {

	// set up the mutex assoicated with the pipe
	pthread_mutex_init (&pipeMutex, NULL);

	// set up the condition variables associated with the pipe
	pthread_cond_init (&producerVar, NULL);
	pthread_cond_init (&consumerVar, NULL);

	// set up the pipe's buffer
	buffered = new (std::nothrow) Record[bufferSize];
	if (buffered == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	totSpace = bufferSize;
	firstSlot = lastSlot = 0;

	// note that the pipe has not yet been turned off
	done = 0;
}

Pipe :: ~Pipe () {
	// free everything up!
	delete [] buffered;

	pthread_mutex_destroy (&pipeMutex);
	pthread_cond_destroy (&producerVar);
	pthread_cond_destroy (&consumerVar);
}


void Pipe :: Insert (Record *insertMe) {
  //cout << "Insert to Pipe. Record. addr = "<< insertMe <<endl;
  //cout << "before inserted. lastSlot % totSpace = " << lastSlot % totSpace <<endl;
	// first, get a mutex on the pipeline
	pthread_mutex_lock (&pipeMutex);

	// next, see if there is space in the pipe for more data; if
	// there is, then do the insertion
	if (lastSlot - firstSlot < totSpace) {
		buffered [lastSlot % totSpace].Consume (insertMe);

	// if there is not, then we need to wait until the consumer
	// frees up some space in the pipeline
	} else {
		pthread_cond_wait (&producerVar, &pipeMutex);
		buffered [lastSlot % totSpace].Consume (insertMe);
	}
	
	// note that we have added a new record
	lastSlot++;

	// signal the consumer who might now want to suck up the new
	// record that has been added to the pipeline
	pthread_cond_signal (&consumerVar);

	// done!
	pthread_mutex_unlock (&pipeMutex);
	//cout << "after inserted. lastSlot % totSpace = " << lastSlot % totSpace <<endl;
}


int Pipe :: Remove (Record *removeMe) {
  //cout << "before remove. firstSlot % totSpace = " << firstSlot % totSpace <<endl;
	// first, get a mutex on the pipeline
	pthread_mutex_lock (&pipeMutex);
	//	cout << "get lock" <<endl;
	// next, see if there is anything in the pipeline; if
	// there is, then do the removal
	if (lastSlot != firstSlot) {
		
		removeMe->Consume (&buffered [firstSlot % totSpace]);
		//	cout << "remove record" <<endl;
	// if there is not, then we need to wait until the producer
	// puts some data into the pipeline
	} else {

		// the pipeline is empty so we first see if this
		// is because it was turned off
		if (done) {
		  
			pthread_mutex_unlock (&pipeMutex);
			return 0;
		}
		//	cout << "wait consumer" <<endl;
		// wait until there is something there
		pthread_cond_wait (&consumerVar, &pipeMutex);
		//	cout << "wake up from waiting" <<endl;
		// since the producer may have decided to turn off
		// the pipe, we need to check if it is still open
		if (done && lastSlot == firstSlot) {
			pthread_mutex_unlock (&pipeMutex);
			return 0;
		}
		
		//cout << "-remove Pipe addr: " << this <<endl;
		//	cout << "Remove from Pipe. Record. addr = "<< removeMe << "; firstSlot % totSpace = "  << firstSlot % totSpace <<endl;
		  removeMe->Consume (&buffered [firstSlot % totSpace]);
	}
	
	// note that we have deleted a record
	firstSlot++;

	// signal the producer who might now want to take the slot
	// that has been freed up by the deletion
	pthread_cond_signal (&producerVar);
	
	// done!
	pthread_mutex_unlock (&pipeMutex);
	return 1;
}


void Pipe :: ShutDown () {

	// first, get a mutex on the pipeline
        pthread_mutex_lock (&pipeMutex);

	// note that we are now done with the pipeline
	done = 1;

	// signal the consumer who may be waiting
	pthread_cond_signal (&consumerVar);

	// unlock the mutex
	pthread_mutex_unlock (&pipeMutex);
	
}
