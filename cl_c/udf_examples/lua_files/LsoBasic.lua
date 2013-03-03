function stackCreate(topRec) 
   local binname = "dirlist" 
   local dirlist = list();
   topRec[binname] = dirlist; 
   if( not aerospike:exists( topRec ) ) then
      info("Create Record()\n");
      rc = aerospike:create( topRec );
   else
      info("Update Record()\n");
      rc = aerospike:update( topRec );
   end
end

function stackPush ( topRec, newValue ) 
   if( not aerospike:exists( topRec ) ) then
      info("stackPush Failed Record not found\n");
      rc = aerospike:create( topRec );
   end 
   local binname    = "dirlist"; 
   local dirlist    = topRec["dirlist"];
   info("Create new Record ");
   newRec           = aerospike:crec_create( topRec );
   newRec["valbin"] = newValue;
   info("Put value in new Record ", tostring( newValue ) );
   info("Update New Record ");
   aerospike:crec_update( topRec, newRec );

   local newdigest  = record.digest( newRec );
   info("Prepend to Top Record ");
   list.prepend (dirlist, tostring( newdigest ));
   info("Put value in Top Record %s", tostring( newdigest ) );
   topRec[binname]  = dirlist;
   info("Update Top Record ");
   aerospike:update( topRec );
end

function stackPeek ( topRec, count ) 
   if( not aerospike:exists( topRec ) ) then
      info("stackPeek Failed Record not found\n");
      rc = aerospike:create( topRec );
   end 
   local binname  = "dirlist"; 
   local dirlist  = topRec[binname];
   info("Dir list state at peek |%s| ", tostring(dirlist));
   local peeklist = list.take(dirlist, count);
   info("Peek size requested %d, peeked %d", count, list.size(peeklist));	
   for index = 1, list.size(peeklist) do
      local valdig = tostring ( dirlist[index] );
      newRec       = aerospike:crec_open( topRec, valdig );
      newValue     = newRec["valbin"];
      info("stackPeek: found %s --> %s", valdig, tostring( newValue ) );
      aerospike:crec_close( topRec, newRec );
   end
end
