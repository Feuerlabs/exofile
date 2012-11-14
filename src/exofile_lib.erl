%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2012, Tony Rogvall
%%% @doc
%%%    File library
%%% @end
%%% Created : 12 Nov 2012 by Tony Rogvall <tony@rogvall.se>

-module(exofile_lib).

-include_lib("kernel/include/file.hrl").

-export([delete_region/3,
	 open_region/3,
	 fill_region/4,
	 insfill_data/4,
	 insert_data/3,
	 write_data/3,
	 append_data/2,
	 prepend_data/2
	]).
-export([write_data_from_file/5,
	 insert_data_from_file/5,
	 fill_data_from_file/6,
	 insfill_data_from_file/6]).
-export([tmpfile/0]).
-export([tmpnam/0, tempnam/2]).
-export([with_open_file/2, with_open_file/3]).

-export([show_region/1, show_region/3]).

-define(FILL_CHUNK_SIZE, 2048).
-define(OPEN_CHUNK_SIZE, 2048).
-define(COPY_CHUNK_SIZE, 2048).
-define(DEL_CHUNK_SIZE, 2048).
-define(SHOW_CHUNK_SIZE, 1024).

%% @doc
%%    Delete a region of a file. The region is specified by an offset
%%    and number of bytes to delete from that offset.
%% @end
-spec delete_region(File::string()|file:io_device(),
		    Offset::non_neg_integer(),
		    Bytes::non_neg_integer()) ->
			   ok | {error, term()}.
			  
delete_region(File, Offset, Bytes) when 
      is_integer(Offset), Offset >= 0,
      is_integer(Bytes), Bytes >= 0 ->
    with_open_file(File, 
		   fun(Fd) -> delete_region_(Fd,Offset,Bytes) end).


%% @doc
%%    Open  a region in a file. The region is specified by an offset
%%    and number of bytes. The opended regions content is undefined 
%%    after the operation.
%% @end
-spec open_region(File::string()|file:io_device(),
		  Offset::non_neg_integer(),
		  Bytes::non_neg_integer()) ->
			 ok | {error, term()}.
			  
open_region(File, Offset, Bytes) when 
      is_integer(Offset), Offset >= 0,
      is_integer(Bytes), Bytes >= 0 ->
    with_open_file(File, 
		   fun(Fd) -> open_region_(Fd,Offset,Bytes) end).
%%
%% @doc
%%    Insert date into a file. The data is inserted into
%%    the file at offset.
%% @end
-spec insert_data(File::string()|file:io_device(),
		  Offset::non_neg_integer(),
		  Data::iolist()) ->
			 ok | {error, term()}.
			  
insert_data(File, Offset, Data) when 
      is_integer(Offset), Offset >= 0 ->
    Data1 = iolist_to_binary(Data),
    with_open_file(File, 
		   fun(Fd) -> insert_data_(Fd,Offset,Data1) end).

%%
%% @doc
%%    append data into a file. The data is written last
%%    in the file.
%% @end
-spec append_data(File::string()|file:io_device(),
		  Data::iolist()) ->
			 ok | {error, term()}.

append_data(File, Data) ->
    Data1 = iolist_to_binary(Data),
    with_open_file(File, 
		   fun(Fd) ->
			   {ok,Offset} = file:position(Fd,eof),
			   write_data_(Fd,Offset,Data1) 
		   end).

%%
%% @doc
%%    prepend data into a file. The data is written last
%%    in the file.
%% @end
-spec prepend_data(File::string()|file:io_device(),
		   Data::iolist()) ->
			  ok | {error, term()}.

prepend_data(File, Data) ->
    Data1 = iolist_to_binary(Data),
    with_open_file(File, fun(Fd) -> insert_data_(Fd,0,Data1) end).

%%
%% @doc
%%    (over)write data into a file. The data is written into
%%    the file at offset.
%% @end
-spec write_data(File::string()|file:io_device(),
		 Offset::non_neg_integer(),
		 Data::iolist()) ->
			ok | {error, term()}.

write_data(File, Offset, Data) when 
      is_integer(Offset), Offset >= 0 ->
    Data1 = iolist_to_binary(Data),
    with_open_file(File, 
		   fun(Fd) -> write_data_(Fd,Offset,Data1) end).

%%
%% @doc
%%    (over)write data into a file. The data is written into
%%    the file at offset.
%% @end
-spec write_data_from_file(File::string()|file:io_device(),
			   Offset::non_neg_integer(),
			   SrcFile::string()|file:io_device(),
			   SrcOffset::non_neg_integer(),
			   SrcLen::non_neg_integer()) ->
				  ok | {error, term()}.

write_data_from_file(File, Offset, SrcFile, SrcOffset, SrcLen) when 
      is_integer(Offset), Offset >= 0,
      is_integer(SrcOffset), SrcOffset >= 0,
      is_integer(SrcLen), SrcLen >= 0 ->
    with_open_file(File, 
      fun(Dst) -> 
        with_open_file(SrcFile,
	  fun(Src) ->
	     copy_data_(Src,SrcOffset,
			Dst,Offset,
			?COPY_CHUNK_SIZE,
			SrcLen)
	  end)
      end).

%%
%% @doc
%%    fill data from file. The data is written into
%%    the file at offset.
%% @end
-spec fill_data_from_file(File::string()|file:io_device(),
			  Offset::non_neg_integer(),
			  Bytes::non_neg_integer(),
			  SrcFile::string()|file:io_device(),
			  SrcOffset::non_neg_integer(),
			  SrcLen::non_neg_integer()) ->
				 ok | {error, term()}.

fill_data_from_file(File, Offset, Bytes, 
		    SrcFile, SrcOffset, SrcLen) when 
      is_integer(Offset), Offset >= 0,
      is_integer(Bytes), Bytes >= 0,
      is_integer(SrcOffset), SrcOffset >= 0,
      is_integer(SrcLen), SrcLen >= 0 ->
    with_open_file(File, 
      fun(Dst) -> 
        with_open_file(SrcFile,
	  fun(Src) ->
		  fill_data_(Src,SrcOffset,SrcLen,?FILL_CHUNK_SIZE,
			     Dst,Offset,Bytes)
	  end)
      end).

%%
%% @doc
%%    insfill data from file. The data is written into
%%    the file at offset.
%% @end
-spec insfill_data_from_file(File::string()|file:io_device(),
			     Offset::non_neg_integer(),
			     Bytes::non_neg_integer(),
			     SrcFile::string()|file:io_device(),
			     SrcOffset::non_neg_integer(),
			     SrcLen::non_neg_integer()) ->
				    ok | {error, term()}.

insfill_data_from_file(File, Offset, Bytes, 
		       SrcFile, SrcOffset, SrcLen) when 
      is_integer(Offset), Offset >= 0,
      is_integer(Bytes), Bytes >= 0,
      is_integer(SrcOffset), SrcOffset >= 0,
      is_integer(SrcLen), SrcLen >= 0 ->
    with_open_file(File, 
      fun(Dst) -> 
        with_open_file(SrcFile,
	  fun(Src) ->
		  case open_region_(Dst,Offset,SrcLen) of
		      ok ->
			  fill_data_(Src,SrcOffset,SrcLen,?FILL_CHUNK_SIZE,
				     Dst,Offset,Bytes);
		      Error ->
			  Error
		  end
	  end)
      end).

%%
%% @doc
%%    Insert fill from a source file into the destination file.
%%    The data is inserted into the destination file at offset.
%% @end
-spec insert_data_from_file(File::string()|file:io_device(),
			    Offset::non_neg_integer(),
			    SrcFile::string()|file:io_device(),
			    SrcOffset::non_neg_integer(),
			    SrcLen::non_neg_integer()) ->
				   ok | {error, term()}.
			  
insert_data_from_file(File, Offset, SrcFile, SrcOffset, SrcLen) when
      is_integer(Offset), Offset >= 0,
      is_integer(SrcOffset), SrcOffset >= 0,
      is_integer(SrcLen), SrcLen >= 0 ->
    with_open_file(File, 
      fun(Dst) ->
        with_open_file(SrcFile,
	  fun(Src) ->
		  case open_region_(Dst,Offset,SrcLen) of
		      ok ->
			  copy_data_(Src,SrcOffset,
				     Dst,Offset,
				     ?COPY_CHUNK_SIZE,
				     SrcLen);
		      Error ->
			  Error
		  end
	  end)
      end).

%% @doc
%%    fill a region of file with datae. The data is written into
%%    the file at offset, if data is to small it is repeated to
%%    fill the region
%% @end
-spec fill_region(File::string()|file:io_device(),
		  Offset::non_neg_integer(),
		  Bytes::non_neg_integer(),
		  Data::iolist()) ->
			 ok | {error, term()}.

fill_region(File, Offset, Bytes, Data) when 
      is_integer(Offset), Offset >= 0,
      is_integer(Bytes), Bytes >= 0 ->
    Data1 = iolist_to_binary(Data),
    with_open_file(File, 
		   fun(Fd) -> fill_region_(Fd,Offset,Bytes,Data1) end).

%%
%% @doc
%%    Insert fill data into a file. The data is inserted into
%%    the file at offset.
%% @end
-spec insfill_data(File::string()|file:io_device(),
		   Offset::non_neg_integer(),
		   Bytes::non_neg_integer(),
		   Data::iolist()) ->
			  ok | {error, term()}.

insfill_data(File, Offset, Bytes, Data) when 
      is_integer(Offset), Offset >= 0,
      is_integer(Bytes), Bytes >= 0 ->
    Data1 = iolist_to_binary(Data),
    with_open_file(File, 
		   fun(Fd) -> 
			   case open_region_(Fd,Offset,Bytes) of
			       ok ->
				   fill_region_(Fd,Offset,Bytes,Data1);
			       Error ->
				   Error
			   end
		   end).

%% @doc
%%    Show data in a file
%% @end
-spec show_region(File::string()|file:io_device(),
		  Offset::non_neg_integer(),
		  Bytes::non_neg_integer()) ->
			 ok | {error, term()}.

show_region(File, Offset, Bytes) when 
      is_integer(Offset), Offset >= 0,
      is_integer(Bytes), Bytes >= 0 ->
    with_open_file(File, 
		   fun(Fd) -> show_region_(Fd,Offset,Bytes) end).

-spec show_region(File::string()|file:io_device()) ->
			 ok | {error, term()}.

show_region(File) ->
    with_open_file(File, 
		   fun(Fd) ->
			   {ok,Bytes} = file:position(Fd, {eof,0}),
			   show_region_(Fd,0,Bytes) 
		   end).




show_region_(_Fd,_Offset,0) ->
    ok;
show_region_(Fd,Offset,Bytes) ->
    N0 = min(Bytes, ?SHOW_CHUNK_SIZE),
    case file:pread(Fd, Offset, N0) of
	{ok,Data} ->
	    N = byte_size(Data),
	    io:put_chars(Data),
	    show_region_(Fd,Offset+N,Bytes-N);
	Error ->
	    Error
    end.

%% @doc
%%   Create a temporary file
%% @end
-spec tmpfile() ->
    {ok,Fd::file:io_decice()} | {error,Reason::term()}.

tmpfile() ->
    file:open(tmpnam(), [read,write,raw,binary]).
    
%% @doc
%%   Create a temporary filename
%% @end
-spec tmpnam() -> string().

tmpnam() ->
    case os:getenv("TMPDIR") of
	false -> tempnam("/tmp", "");
	""    -> tempnam("/tmp", "");
	Dir   -> tempnam(Dir, "")
    end.

%% @doc
%%   Create a temporary filename in a directory
%% @end
-spec tempnam(Dir::string(), Pfx::string()) -> string().

tempnam(Dir, Pfx) ->
    tempnam_(Dir, Pfx, 100).

tempnam_(_Dir, _Pfx, 0) ->
    erlang:error(too_many_attempts);
tempnam_(Dir, Pfx, I) when I > 0 ->
    <<N:32>> = crypto:strong_rand_bytes(4),
    Name = tl(integer_to_list(1000000000 + (N rem 1000000000))),
    FileName = filename:join(Dir, Pfx++Name),
    case filelib:is_file(FileName) of
	true -> tempnam_(Dir,Pfx,I-1);
	false -> FileName
    end.

%% @doc
%%   Apply Fun to an open file descriptor
%%   The File argument is opened if for read/wrute if needed
%% @end

-spec with_open_file(File::string()|file:io_device(),
		     Fun::fun((Fd::file:io_device()) -> term())) ->
			    ok | {error, term()}.

with_open_file(File, Fun) ->
    with_open_file(File, [binary,raw,read,write], Fun).

-spec with_open_file(File::string()|file:io_device(),
		     Modes::[file:mode()],
		     Fun::fun((Fd::file:io_device()) -> term())) ->
			    ok | {error, term()}.

with_open_file(File,Modes,Fun) ->
    if is_pid(File) ->
	    Fun(File);
       is_record(File, file_descriptor) ->
	    Fun(File);
       true ->
	    case file:open(File, Modes) of
		{ok,Fd} ->
		    try Fun(Fd) of
			Result -> Result
		    catch
			error:Reason ->
			    erlang:raise(error, Reason, 
					 erlang:get_stacktrace())
		    after
			file:close(Fd)
		    end;
		Error ->
		    Error
	    end
    end.
    

%%
%% Utils
%%

fill_region_(_Fd,_Offset,0,_Data) -> 
    ok;
fill_region_(Fd,Offset,Bytes,Data) ->
    Data1 = bin_duplicate(Data, ?FILL_CHUNK_SIZE),
    file:position(Fd, Offset),
    fill_region_(Fd, Bytes, Data1).
    
fill_region_(_Fd, 0, _FillPattern) ->
    ok;
fill_region_(Fd, Len, FillPattern) ->
    BSize = byte_size(FillPattern),
    if Len < BSize ->
	    <<Data:Len/binary,_/binary>> = FillPattern,
	    case file:write(Fd, Data) of
		ok -> 
		    fill_region_(Fd, 0, FillPattern);
		Error -> 
		    Error
	    end;
       true ->
	    case file:write(Fd, FillPattern) of
		ok ->
		    fill_region_(Fd, Len-BSize, FillPattern);
		Error -> 
		    Error
	    end
    end.

%% create a duplicate of binary not longer than Size unless
bin_duplicate(Bin, Size) ->
    BSize = byte_size(Bin),
    if BSize >= Size -> 
	    Bin;
       BSize > 0 ->
	    N = Size div BSize,
	    list_to_binary(lists:duplicate(N, Bin))
    end.

write_data_(Fd,Offset,Bytes) ->
    %% io:format("pwrite: offs=~w, data=~w\n", [Offset,Bytes]),
    file:pwrite(Fd, Offset, Bytes).

read_data_(Fd,Offset,Size) ->
    %% io:format("pread: offs=~w, size=~w\n", [Offset,Size]),
    file:pread(Fd,Offset,Size).


%% delete region from offset and size bytes
delete_region_(Fd,Offset,Size) ->
    case copy_data_eof_(Fd,Offset+Size,Fd,Offset,?DEL_CHUNK_SIZE) of
	eof ->
	    file:truncate(Fd);
	Error ->
	    Error
    end.

insert_data_(Fd,Offset,Bytes) ->
    case open_region_(Fd,Offset,byte_size(Bytes)) of
	ok ->
	    write_data_(Fd,Offset,Bytes);
	Error ->
	    Error
    end.
    

%% open up a region from offst and size bytes
open_region_(Fd, Offset, Size) ->
    {ok,FileSize} = file:position(Fd, {eof,0}),
    MoveSize = FileSize-Offset,
    Chunk = min(MoveSize, ?OPEN_CHUNK_SIZE),
    case copy_data_bof_(MoveSize,
			Fd,FileSize-Chunk,
			Fd,FileSize+Size-Chunk,
			Chunk) of
	ok -> 
	    ok;
	Error ->
	    Error
    end.


copy_data_bof_(0,_SrcFd,_SrcOffset,_DstFd,_DstOffset,_Chunk) ->
    ok;
copy_data_bof_(Size,SrcFd,SrcOffset,DstFd,DstOffset,Chunk) when Size>0 ->
    N0 = min(Size,Chunk),
    case read_data_(SrcFd,SrcOffset,N0) of
	{ok,Data} ->
	    case write_data_(DstFd,DstOffset,Data) of
		ok ->
		    N = byte_size(Data),
		    copy_data_bof_(Size-N,
				   SrcFd,SrcOffset-N,
				   DstFd,DstOffset-N,Chunk);
		Error ->
		    Error
	    end;
	Error ->
	    Error
    end.

copy_data_eof_(SrcFd,SrcOffset,DstFd,DstOffset,Chunk) ->
    case read_data_(SrcFd,SrcOffset,Chunk) of
	eof ->
	    file:position(DstFd,DstOffset),
	    eof;
	{ok,Data} ->
	    case write_data_(DstFd,DstOffset,Data) of
		ok ->
		    N = byte_size(Data),
		    copy_data_eof_(SrcFd,SrcOffset+N,
				   DstFd,DstOffset+N,Chunk);
		Error ->
		    Error
	    end;
	Error ->
	    Error
    end.

copy_data_(_SrcFd,_SrcOffset,_DstFd,_DstOffset,_Chunk,0) ->
    ok;
copy_data_(SrcFd,SrcOffset,DstFd,DstOffset,Chunk,Size) ->
    N0 = min(Size,Chunk),
    case read_data_(SrcFd,SrcOffset,N0) of
	{ok,Data} ->
	    case write_data_(DstFd,DstOffset,Data) of
		ok ->
		    N = byte_size(Data),
		    copy_data_(SrcFd,SrcOffset+N,
			       DstFd,DstOffset+N,Chunk,Size-N);
		Error ->
		    Error
	    end;
	Error ->
	    Error
    end.

fill_data_(SrcFd,SrcOffset,SrcLen,Chunk,DstFd,DstOffset,DstLen) ->
    if SrcLen =< ?FILL_CHUNK_SIZE; DstLen =< ?FILL_CHUNK_SIZE ->
	    case file:pread(SrcFd,SrcOffset,SrcLen) of
		{ok,Data} ->
		    fill_region(DstFd,DstOffset,Data,DstLen);
		Error ->
		    Error
	    end;
       true ->
	    fill_data_(SrcFd,SrcOffset,SrcLen,SrcOffset,SrcLen,
		       Chunk,DstFd,DstOffset,DstLen)
    end.


fill_data_(_SrcFd,_SrcOffset0,_SrcLen0,_SrcOffset,_SrcLen,
	   _Chunk,_DstFd,_DstOffset,0) ->
    ok;
fill_data_(SrcFd,SrcOffset0,SrcLen0,_SrcOffset,0,
	   Chunk,DstFd,DstOffset,DstLen) ->
    fill_data_(SrcFd,SrcOffset0,SrcLen0,SrcOffset0,SrcLen0,
	       Chunk,DstFd,DstOffset,DstLen);
fill_data_(SrcFd,SrcOffset0,SrcLen0,SrcOffset,SrcLen,
	   Chunk,DstFd,DstOffset,DstLen) ->    
    N0 = min(DstLen,min(SrcLen,Chunk)),
    case read_data_(SrcFd,SrcOffset,N0) of
	{ok,Data} ->
	    case write_data_(DstFd,DstOffset,Data) of
		ok ->
		    N = byte_size(Data),
		    fill_data_(SrcFd,SrcOffset0,SrcLen0,
			       SrcOffset+N,SrcLen-N,
			       Chunk,
			       DstFd,DstOffset+N,DstLen-N);
		Error ->
		    Error
	    end;
	Error ->
	    Error
    end.

