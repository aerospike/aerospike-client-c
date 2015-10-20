provider aerospike {
   probe put__execute_starting(uint64_t);
   probe put__execute_finished(uint64_t);
   probe query__foreach_starting(uint64_t);
   probe query__foreach_finished(uint64_t);
   probe query__enqueue_task(uint64_t, char *);
   probe query__command_execute(uint64_t, char *);
   probe query__command_complete(uint64_t, char *);
   probe query__command_complete(uint64_t, char *);
   probe query__parse_records_starting(uint64_t, char *, size_t)
   probe query__parse_records_finished(uint64_t, char *, size_t, int)
   probe query__aggparse_starting(uint64_t, char *)
   probe query__aggparse_finished(uint64_t, char *)
   probe query__aggcb_starting(uint64_t, char *)
   probe query__aggcb_finished(uint64_t, char *)
   probe query__recparse_starting(uint64_t, char *)
   probe query__recparse_bins(uint64_t, char *)
   probe query__recparse_finished(uint64_t, char *)
   probe query__reccb_starting(uint64_t, char *)
   probe query__reccb_finished(uint64_t, char *)
};
