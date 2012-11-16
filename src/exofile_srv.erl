%%%-------------------------------------------------------------------
%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2012, Tony Rogvall
%%% @doc
%%%     exofile file handle server
%%% @end
%%% Created :  6 Nov 2012 by Tony Rogvall <tony@rogvall.se>
%%%-------------------------------------------------------------------
-module(exofile_srv).

-behaviour(gen_server).
-compile(export_all).
%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 
-define(MAX_FILE_SIZE,  0).        %% no limit
-define(MAX_CHUNK_SIZE, 1024).     %% random limit - fix this
-define(MIN_CHUNK_SIZE, 256).      %% a random limit
-define(MAX_OPEN_TIME, 60*1000).   %% max file open time
-define(MAX_OPEN_FILES, 20).       %% max number of open files

-define(FILL_CHUNK_SIZE, 2048).
-define(COPY_CHUNK_SIZE, 2048).
-define(INSERT_CHUNK_SIZE, 2048).


-record(file_meta,
	{
	  file_size = 0      :: non_neg_integer(),
	  chunk_size = 1024  :: non_neg_integer(),
	  file_type  = "application/octet-stream" :: string(),
	  file_name  = ""    :: string(),
	  file_mode  = write  :: read | write | read_write
	}).
%%
%% other file_type's could be:
%% application/x-deb  for debian packages
%% 

-record(file_handle,
	{
	  id,   %% file id (name)
	  fd,   %% file descriptor to open file (undefined = closed)
	  ts,   %% last activity timestamp
	  iset, %% defined ranges of the file
	  meta  %% meta info
	}).

-record(state, 
	{
	  dir :: string(),     %% download place (priv/download)
	  tmr :: reference(),  %% inactivity timer, cache cleaning
	  cache = [] :: #file_handle{}
	}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    Dir = filename:join(code:priv_dir(exofile), "downloads"),
    %% maybe create if needed
    Cache = cache_load(Dir),
    {ok, #state{ dir = Dir, cache = Cache } }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({'chunk-write', ID, Offset, Data},_From,State) ->
    case cache_open(ID, State) of
	{{ok,H},State1} ->
	    Element = {write,Offset,byte_size(Data),Data},
	    {Reply,State2} = chunk_save(H, Element,State1),
	    {reply, Reply, State2};
	{Reply, State1} ->
	    {reply, Reply, State1}
    end;
handle_call({'chunk-insert', ID, Offset, Data},_From,State) ->
    case cache_open(ID, State) of
	{{ok,H},State1} ->
	    Element = {insert,Offset,byte_size(Data),Data},
	    {Reply,State2} = chunk_save(H, Element, State1),
	    {reply, Reply, State2};
	{Reply, State1} ->
	    {reply, Reply, State1}
    end;
handle_call({'chunk-delete', ID, Offset, Size},_From,State) ->
    case cache_open(ID, State) of
	{{ok,H},State1} ->
	    {Reply,State2} = chunk_save(H, {delete,Offset,Size}, State1),
	    {reply, Reply, State2};
	{Reply, State1} ->
	    {reply, Reply, State1}
    end;
handle_call({'chunk-fill', ID, Offset, Size, Data},_From,State) ->
    case cache_open(ID, State) of
	{{ok,H},State1} ->
	    {Reply,State2} = chunk_save(H, {fill,Offset,Size,Data},State1),
	    {reply, Reply, State2};
	{Reply, State1} ->
	    {reply, Reply, State1}
    end;
handle_call({'chunk-insfill', ID, Offset, Size, Data},_From,State) ->
    case cache_open(ID, State) of
	{{ok,H},State1} ->
	    {Reply,State2} = chunk_save(H, {insfill,Offset,Size,Data},State1),
	    {reply, Reply, State2};
	{Reply, State1} ->
	    {reply, Reply, State1}
    end;
handle_call({'chunk-read', ID, _Offset, _Size},_From,State) ->
    %% only for read only files
    case cache_open(ID, State) of
	{{ok,_H},State1} ->
	    %% FIXME:!
	    {reply, {error,not_implemented_yet}, State1};
	{Reply, State1} ->
	    {reply, Reply, State1}
    end;

handle_call({'chunk-list-transfered', ID, N, Offset},_From,State) ->
    case cache_find(ID, State) of
	{ok, H} ->
	    Items = get_chunk_list(N, Offset, H#file_handle.iset),
	    {reply, {ok, [{'ranges', Items}]}, State};
	Error ->
	    {reply, Error, State}
    end;
handle_call({'chunk-list-missing',ID,N,Offset,FileSize},_From,State) ->
    case cache_find(ID, State) of
	{ok, H} ->
	    case get_missing_chunks(H, FileSize) of
		Error = {error, _} ->
		    {reply, Error, State};
		{ok,Missing} ->
		    Items = get_chunk_list(N, Offset, Missing),
		    {reply, {ok, [{'ranges', Items}]}, State}
	    end;
	Error ->
	    {reply, Error, State}
    end;
handle_call({'transfer-init',
	     FileSizeHint,    %% non_neg_integer(), 0 if unknown
	     ChunkSizeHint,   %% non_neg_integer(), 0 if unknown
	     FileType,        %% string(), MIME type indicating file contents
	     FileNameHint,    %% string(), local name hint, "" if unknown
	     FileMode         %% read|write
	    }, _From, State) ->
    Dir = State#state.dir,
    ChunkSize = max(?MIN_CHUNK_SIZE,
		    min(pow2(ChunkSizeHint), pow2(?MAX_CHUNK_SIZE))),
    Meta = #file_meta {file_size=FileSizeHint, 
		       chunk_size=ChunkSize,
		       file_type=FileType,
		       file_name=FileNameHint,
		       file_mode=FileMode},
    ID = create_file_id(Dir),
    case open_file(Dir, ID, FileMode) of
	{ok, Fd} ->
	    %% Fixme handle read/read_write (copy a NameHint file...)
	    H = #file_handle { id=ID, fd=Fd, ts=timestamp(), 
			       iset=exofile_iset:new(), meta=Meta},
	    case chunk_save(H, {meta, FileSizeHint, ChunkSize, FileType,
				FileNameHint, FileMode}, State) of
		{ok, State1} ->
		    {reply, {ok,[{'transfer-id',ID},
				 {'chunk-size',ChunkSize}]}, State1};
		Error ->
		    close_file(Fd),
		    delete_file(Dir, ID),
		    {reply, Error, State}
	    end;
	Error ->
	    {reply, Error, State}
    end;

handle_call({'transfer-final',
	     ID,               %% file cache id
	     N,                %% max list of missing chunks if not ready
	     FileSize,         %% non_neg_integer(), Final file size, 
	     FileName,         %% string(), local name hint, "" if unknown
	     SHA1              %% string()  hex asci SHA1 over file data
	    }, _From, State) ->
    case cache_find(ID, State) of
	{ok,H} ->
	    case get_missing_chunks(H, FileSize) of
		{error,Reason} -> %% atom | string?
		    {reply, {error,Reason}, State};
		{ok,[]} ->
		    %% move chunks to "final" destination
		    %% should probably run this in a process !
		    case create_target(H, SHA1, FileName, State) of
			ok ->
			    State1 = cache_delete(H#file_handle.id, State),
			    {reply, ok, State1};
			Error ->
			    {reply, Error, State}
		    end;
		{ok,Missing} ->
		    Items = get_chunk_list(N, 0, Missing),
		    {reply, {missing, [{'ranges',Items}]}, State}
	    end;
	{Error,State1} ->
	    {reply, Error, State1}
    end;
handle_call({'transfer-list',N,Prev},_From,State) ->
    Items = get_transfer_list(N, Prev, State#state.cache),
    {reply, {ok, [{'transfers', Items}]}, State};
    
    
handle_call(stop, _From, State) ->
    %% flush and close all
    {stop,normal,ok,State};
    
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_target(H, SHA1, FileName, State) ->
    if H#file_handle.fd =:= undefined ->
	    case open_file(State#state.dir, H#file_handle.id, read_write) of
		{ok,Fd} ->
		    Res = create_from_chunks(Fd, SHA1, FileName),
		    file:close(Fd),
		    Res;
		Error -> Error
	    end;
       true ->
	    create_from_chunks(H#file_handle.fd, SHA1, FileName)
    end.

create_from_chunks(Fd, _SHA1, FileName) ->
    TempName = exofile_lib:tmpnam(),
    try run_file_chunks(Fd, TempName) of
	ok ->
	    io:format("Rename(not): ~s => ~s\n", [TempName, FileName]),
	    ok;
	Error ->
	    Error
    catch
	error:Reason ->
	    {error, Reason} %% may be not
    end.


get_file_size(H, 0) ->
    case (H#file_handle.meta)#file_meta.file_size of
	0 -> exofile_iset:last(H#file_handle.iset) + 1;
	Size -> Size
    end;
get_file_size(_H, Size) ->
    Size.
    
%% pow2 - return nearest power of 2 less than X
pow2(0) -> 0;
pow2(N) when is_integer(N), N > 0 -> pow2(1, N).

pow2(X,N) ->
    X1 = X bsl 1,
    if X1 > N -> X;
       X1 =:= N -> X1;
       true -> pow2(X1,N)
    end.

%% create a random filename range "000000" - "999999"
create_file_id(Dir) ->
    ID =  tl(integer_to_list((random:uniform(1000000)-1) + 1000000)),
    FileName = filename:join(Dir, ID),
    case filelib:is_file(FileName) of
	false -> ID;
	true -> create_file_id(Dir)
    end.

open_file(Dir, ID, read) ->
    file:open(filename:join(Dir, ID), [read,binary]);
open_file(Dir, ID, read_write) ->
    file:open(filename:join(Dir, ID), [read,write,binary]);
open_file(Dir, ID, write) ->
    file:open(filename:join(Dir, ID), [read,write,binary]).

close_file(undefined) ->
    ok;
close_file(Fd) ->
    file:close(Fd).

delete_file(Dir, ID) ->
    file:delete(filename:join(Dir,ID)).

cache_find(ID, State) ->
    case lists:keyfind(ID, #file_handle.id, State#state.cache) of
	false ->
	    {error, enoent};
	H ->
	    {ok,H}
    end.

%% set or insert new file handle into cache
cache_set(H, State) ->
    ID = H#file_handle.id, 
    case lists:keymember(ID, #file_handle.id, State#state.cache) of
	true ->
	    Cache = lists:keyreplace(ID, #file_handle.id, State#state.cache, H),
	    State#state { cache = Cache };
	false ->
	    Cache = State#state.cache ++ [H],
	    State#state { cache = Cache }
    end.


%% close file assoicated with ID, but keep in cache
cache_close(ID, State) ->
    case lists:keytake(ID, #file_handle.id, State#state.cache) of
	false ->
	    State;
	{value,H,Hs} ->
	    close_file(H#file_handle.fd),
	    H1 = H#file_handle { fd = undefined },
	    State#state { cache = [H1|Hs]}
    end.

%% close and delete file associate with ID.
cache_delete(ID, State) ->
    case lists:keytake(ID, #file_handle.id, State#state.cache) of
	false ->
	    State;
	{value,H,Hs} ->
	    close_file(H#file_handle.fd),
	    delete_file(State#state.dir, ID),
	    State#state { cache = Hs}
    end.

%% open / reopen file if needed
cache_open(ID, State) ->
    case lists:keytake(ID, #file_handle.id, State#state.cache) of
	false ->
	    {{error,enoent}, State};
	{value,H,Cache} ->
	    Mode = (H#file_handle.meta)#file_meta.file_mode,
	    if H#file_handle.fd =:= undefined ->
		    case open_file(State#state.dir, ID, Mode) of
			{ok,Fd} ->
			    H1 = H#file_handle { fd=Fd, ts=timestamp()},
			    {{ok,H1}, State#state { cache = [H1|Cache]}};
			Error ->
			    {Error, State}
		    end;
	       true ->
		    H1 = H#file_handle { ts=timestamp()},
		    {{ok,H1}, State#state { cache = [H1|Cache]}}
	    end
    end.

%% update last file activity
cache_activity(ID, State) ->
    case lists:keytake(ID, #file_handle.id, State#state.cache) of
	false ->
	    State;
	{value,H,Cache} ->
	    H1 = H#file_handle { ts = timestamp() },
	    State#state { cache = [H1|Cache]}
    end.

%% load file cache items - do not open any (yet)
cache_load(Dir) ->
    case file:list_dir(Dir) of
	{ok, Files} ->
	    cache_load_items(Dir, lists:sort(Files), [], []);
	Error ->
	    io:format("cache_load: error=~p\n", [Error]),
	    []
    end.

cache_load_items(Dir, [File|Files], Lost, Cache) ->
    case is_cache_file(File) of
	true ->
	    case load_cache_file(Dir, File) of
		{ok, H} ->
		    cache_load_items(Dir, Files, Lost, [H|Cache]);
		_Error ->
		    io:format("Error loading ~s : ~p\n", [File,_Error]),
		    cache_load_items(Dir, Files, [File|Lost], Cache)
	    end;
	false ->
	    cache_load_items(Dir, Files, Lost, Cache)
    end;
cache_load_items(_Dir, [], [], Cache) ->
    Cache;
cache_load_items(_Dir, [], Lost, Cache) ->
    io:format("Lost files: ~p\n", [Lost]),
    Cache.

load_cache_file(Dir, ID) ->
    case file:open(filename:join(Dir, ID), [raw,read,binary]) of
	{ok,Fd} ->
	    H = #file_handle { id=ID, iset=exofile_iset:new()},
	    Result = load_cache_fd(Fd, H),
	    file:close(Fd),
	    Result;
	Error ->
	    Error
    end.

%% FIXME: make this more robust. When we encouter a truncated record
%% a bad crc, we skip could truncate the file and keep the things we
%% have read sofar. report that data back.
load_cache_fd(Fd, H) ->
    case file:read(Fd, 5) of
	eof -> {ok, H};
	{ok, <<0,Size:32>>} ->
	    case file:read(Fd, Size+4) of
		{ok, <<Bin:Size/binary,CRC:32 >>} ->
		    case erlang:crc32(Bin) of
			CRC ->
			    E = binary_to_term(Bin),
			    H1 = update_cache_iset(E, H),
			    load_cache_fd(Fd,H1);
			_ ->
			    {error, bad_crc}
		    end;
		{ok, _} ->
		    io:format("truncated 1:\n", []),
		    {error, truncated_record};
		Error ->
		    Error
	    end;
	{ok, <<1,Size:32>>} ->
	    %% skip raw binary data (maybe verify this?)
	    file:position(Fd, {cur, Size+4}),
	    load_cache_fd(Fd, H);
	{ok, _} ->
	    io:format("truncated 2:\n", []),
	    {error, truncated_record}
    end.


update_cache_iset({meta,FileSizeHint,ChunkSize,FileType,
		   FileNameHint,FileMode}, H) ->
    Meta = #file_meta {file_size=FileSizeHint, 
		       chunk_size=ChunkSize,
		       file_type=FileType, 
		       file_name=FileNameHint, 
		       file_mode=FileMode},
    H#file_handle { meta=Meta };
update_cache_iset(Operation, H) ->
    ISet = update_iset(Operation, H#file_handle.iset),
    H#file_handle { iset = ISet }.

update_iset(Operation, ISet) ->
    case Operation of
	{write,Offset,Size,_Data} ->
	    End = Offset + Size-1,
	    A = exofile_iset:from_list([{Offset,End}]),
	    exofile_iset:union(ISet,A);

	{insert,Offset,Size,_Data} ->
	    End = Offset + Size - 1,
	    A = exofile_iset:less(Offset, ISet),
	    B = exofile_iset:from_list([{Offset,End}]),
	    C0 = exofile_iset:greater(Offset-1, ISet),
	    C  = exofile_iset:offset(Size, C0),
	    exofile_iset:union(A, exofile_iset:union(B, C));

	{delete,Offset,Size} ->
	    End = Offset + Size -1,
	    A = exofile_iset:less(Offset, ISet),
	    C = exofile_iset:greater(End-1, ISet),
	    C1  = exofile_iset:offset(-Size, C),
	    exofile_iset:union(A, C1);

	{fill,Offset,Size,_Data} ->
	    End = Offset + Size - 1,
	    A = exofile_iset:from_list([{Offset,End}]),
	    exofile_iset:union(ISet,A);

	{insfill,Offset,Size,_Data} ->
	    End = Offset + Size - 1,
	    A = exofile_iset:less(Offset, ISet),
	    B = exofile_iset:from_list([{Offset,End}]),
	    C0 = exofile_iset:greater(Offset-1, ISet),
	    C  = exofile_iset:offset(Size, C0),
	    exofile_iset:union(A, exofile_iset:union(B, C));

	{meta,_FileSizeHint,_ChunkSize,_FileType,_FileNameHint,_FileMode} ->
	    ISet
    end.
	
is_cache_file(File) ->
    case File of
	[A,B,C,D,E,F] ->
	    try list_to_integer([A,B,C,D,E,F]) of
		_N -> true
	    catch
		error:_ ->
		    false
	    end;
	_ ->
	    false
    end.

%% given FileSize (if 0 then check meta and then iset)
get_missing_chunks(H, FileSize) ->
    case get_file_size(H, FileSize) of
	0 -> 
	    {error, badarg};
	FileSize1 ->
	    Universe = exofile_iset:from_list([{0,FileSize1-1}]),
	    Missing  = exofile_iset:difference(Universe, 
					       H#file_handle.iset),
	    {ok,Missing}
    end.

%% get N ranges interval given a starting point
get_chunk_list(_N, _Offset, []) ->
    [];
get_chunk_list(N, Offset, ISet) ->
    Mask = exofile_iset:from_list([{Offset,exofile_iset:last(ISet)}]),
    A    = exofile_iset:intersect(Mask, ISet),
    %% add to api? but this is a bit special
    make_range_items(1,lists:sublist(A, N)).

make_range_items(I, [{A,B}|Ds]) ->
    [{I,A,(B-A)+1}| make_range_items(I+1,Ds)];
make_range_items(I, [A|Ds]) ->
    [{I,A,1} | make_range_items(I+1,Ds)];
make_range_items(_I, []) ->
    [].



%% Get N transfer id's (on going transfers)
get_transfer_list(N, "", Hs) ->
    [H#file_handle.id || H <- lists:sublist(Hs, N)];
get_transfer_list(N, Prev, [H|Hs]) when Prev =:= H#file_handle.id ->
    [Hi#file_handle.id || Hi <- lists:sublist(Hs, N)];
get_transfer_list(N, Prev, [_|Hs]) ->
    get_transfer_list(N, Prev, Hs);
get_transfer_list(_N, _Prev, []) ->
    [].

timestamp() ->    
    {M,S,U} = os:timestamp(),
    (M*1000000 + S)*1000000 + U.

chunk_save(H, Term, State) ->
    case chunk_save(H, Term) of
	{ok, H1} ->
	    {ok, cache_set(H1, State)};
	Error ->
	    {Error, State}
    end.

chunk_save(H, C={meta,
		 _FileSizeHint, 
		 _ChunkSize, 
		 _FileType,
		 _FileNameHint, 
		 _FileMode}) ->
    chunk_save_(H, C, undefined);
chunk_save(H, {write,Offset,Size,Data}) when
      is_integer(Offset), Offset>=0, is_binary(Data) ->
    chunk_save_(H, {write,Offset,Size,'_'}, Data);
chunk_save(H, {insert,Offset,Size,Data}) when
      is_integer(Offset), Offset>=0, is_binary(Data) -> 
    chunk_save_(H, {insert,Offset,Size,'_'}, Data);
chunk_save(_H, {delete, _Offset, 0}) ->
    ok;
chunk_save(H, {delete, Offset, Size}) when
      is_integer(Offset), Offset>=0,
      is_integer(Size), Size > 0 ->
    chunk_save_(H, {delete,Offset,Size},undefined);
chunk_save(H, {fill, Offset, Size, Data}) when
      is_integer(Offset), Offset>=0,
      is_integer(Size), Size > 0,
      is_binary(Data) ->
    chunk_save_(H, {fill,Offset,Size,'_'}, Data);
chunk_save(H, {insfill, Offset, Size, Data}) when
      is_integer(Offset), Offset>=0,
      is_integer(Size), Size > 0,
      is_binary(Data) ->
    chunk_save_(H, {insfill,Offset,Size,'_'}, Data).

%% Append the chunk to the log file
chunk_save_(H, Term, Data) ->
    Fd   = H#file_handle.fd,
    case save_bin_chunk(Fd, 0, term_to_binary(Term)) of
	ok -> 
	    case save_bin_chunk(Fd, 1, Data) of
		ok ->
		    {ok, update_cache_iset(Term, H)};
		Error -> 
		    Error
	    end;
	Error -> Error
    end.

save_bin_chunk(_Fd, _Tag, undefined) ->
    ok;
save_bin_chunk(Fd, Tag, Bin) ->
    Size   = byte_size(Bin),
    CRC    = erlang:crc32(Bin),
    file:write(Fd, <<Tag, Size:32, Bin:Size/binary, CRC:32>>).

%%
%% The idea here is to create temporary file, then
%% run the "commands" found in SrcFd and thereby creating
%% a real file. Then this event is signaled to event subsribers
%%
run_file_chunks(SrcFd, Target) ->
    file:position(SrcFd, 0),
    TempFileName = Target,  %% fixme
    exofile_lib:with_open_file(
      TempFileName, fun(Fd) -> run_chunks_(SrcFd, Fd) end).

%%
%% execute file operations
%%
run_chunks_(SrcFd, DstFd) ->
    case file:read(SrcFd, 5) of
	eof -> ok;
	{ok, <<0,Size:32>>} ->
	    case file:read(SrcFd, Size+4) of
		{ok, <<Bin:Size/binary,CRC:32 >>} ->
		    case erlang:crc32(Bin) of
			CRC ->
			    E = binary_to_term(Bin),
			    run_chunks_(SrcFd, E, DstFd);
			_ ->
			    {error, bad_crc}
		    end;
		{ok, _} ->
		    {error, truncated_record};
		Error ->
		    Error
	    end;
	{ok, <<1,_Size:32>>} ->
	    {error, orphan_binary};
	{ok, _} ->
	    {error, truncated_record};
	Error ->
	    Error
    end.

run_chunks_(SrcFd, {meta, _FileSizeHint, _ChunkSize, _FileType,
		    _FileNameHint, _FileMode}, DstFd) ->
    %% fixme: this should be the first chunk in the file:
    %% - if FileType is set then this info is used to target the
    %%   file transfer subscribers.
    %% - if FileSize is set then local file system should be checked
    %%   to see if target file fit.
    %% - FileMode original mode of file
    %%
    run_chunks_(SrcFd, DstFd);

run_chunks_(SrcFd, {write,Offset,SrcSize,'_'}, DstFd) ->
    case file:read(SrcFd, 5) of
	{ok, <<1,SrcSize:32>>} ->
	    if SrcSize =< ?COPY_CHUNK_SIZE ->
		    case run_chunk_copy_bin_(SrcFd, SrcSize, DstFd, Offset) of
			ok ->
			    run_chunks_(SrcFd, DstFd);
			Error ->
			    Error
		    end;
	       true ->
		    case run_chunk_copy_file_(SrcFd,SrcSize,DstFd,Offset) of
			ok ->
			    run_chunks_(SrcFd, DstFd);
			Error ->
			    Error
		    end
	    end;
	{ok, _} ->
	    {error, truncated_record};
	Error ->
	    Error
    end;
run_chunks_(SrcFd, {insert,Offset,SrcSize,'_'}, DstFd) ->
    case file:read(SrcFd, 5) of
	{ok, <<1,SrcSize:32>>} ->
	    if SrcSize =< ?INSERT_CHUNK_SIZE ->
		    case run_chunk_insert_bin_(SrcFd, SrcSize, DstFd, Offset) of
			ok ->
			    run_chunks_(SrcFd, DstFd);
			Error ->
			    Error
		    end;
	       true ->
		    case run_chunk_insert_file_(SrcFd,SrcSize,DstFd,Offset) of
			ok ->
			    run_chunks_(SrcFd, DstFd);
			Error ->
			    Error
		    end
	    end;
	{ok, _} ->
	    {error, truncated_record};
	Error ->
	    Error
    end;
run_chunks_(SrcFd, {fill,Offset,Size,'_'}, DstFd) ->
    %% two cases FillPattern fit into memory (2K or so)
    %% then: read pattern and extend to 2K
    case file:read(SrcFd, 5) of
	{ok, <<1,SrcSize:32>>} ->
	    if SrcSize =< ?FILL_CHUNK_SIZE ->
		    case run_chunk_fill_bin_(SrcFd,SrcSize,DstFd,Offset,Size) of
			ok ->
			    run_chunks_(SrcFd, DstFd);
			Error ->
			    Error
		    end;
	       true ->
		    case run_chunk_fill_file_(SrcFd,SrcSize,DstFd,Offset,Size) of
			ok ->
			    run_chunks_(SrcFd, DstFd);
			Error ->
			    Error
		    end
	    end;
	{ok,_} ->
	    {error, truncated_record};
	Error ->
	    Error
    end;
run_chunks_(SrcFd, {insfill,Offset,Size,'_'}, DstFd) ->
    %% two cases FillPattern fit into memory (2K or so)
    %% then: read pattern and extend to 2K
    case file:read(SrcFd, 5) of
	{ok, <<1,SrcSize:32>>} ->
	    if SrcSize =< ?FILL_CHUNK_SIZE ->
		    case run_chunk_insfill_bin_(SrcFd,SrcSize,DstFd,Offset,Size) of
			ok ->
			    run_chunks_(SrcFd, DstFd);
			Error ->
			    Error
		    end;
	       true ->
		    case run_chunk_insfill_file_(SrcFd,SrcSize,DstFd,Offset,Size) of
			ok ->
			    run_chunks_(SrcFd, DstFd);
			Error ->
			    Error
		    end
	    end;
	{ok,_} ->
	    {error, truncated_record};
	Error ->
	    Error
    end;
run_chunks_(SrcFd, {delete,Offset,Size}, DstFd) ->
    case exofile_lib:delete_region(DstFd,Offset,Size) of
	ok ->
	    run_chunks_(SrcFd, DstFd);
	Error ->
	    Error
    end.



%% SrcSize is small so first do a read and then write
run_chunk_copy_bin_(SrcFd, SrcSize, DstFd, DstOffset) ->
    case file:read(SrcFd, SrcSize+4) of
	{ok, <<Data:SrcSize/binary,CRC:32 >>} ->
	    case erlang:crc32(Data) of
		CRC ->
		    exofile_lib:write_data(DstFd, DstOffset, Data);
		_ ->
		    {error, bad_crc}
	    end;
	{ok, _} ->
	    {error, truncated_record};
	Error ->
	    Error
    end.

%% Move SrcSize data from SrcFd (cur) to DstFd at DstOffset
run_chunk_copy_file_(SrcFd, SrcSize, DstFd, DstOffset) ->
    {ok,SrcOffset} = file:position(SrcFd, cur),
    case exofile_lib:write_data_from_file(DstFd,DstOffset,
					  SrcFd,SrcOffset,SrcSize) of
	ok -> %% fixme return crc32 over data!
	    %% fixme: check crc!
	    case file:position(SrcFd, SrcOffset+SrcSize+4) of
		{ok,_} -> ok;
		Error -> Error
	    end;
	Error ->
	    Error
    end.

%% SrcSize is small so first do a read and then insert
run_chunk_insert_bin_(SrcFd, SrcSize, DstFd, DstOffset) ->
    case file:read(SrcFd, SrcSize+4) of
	{ok, <<Data:SrcSize/binary,CRC:32 >>} ->
	    case erlang:crc32(Data) of
		CRC ->
		    exofile_lib:insert_region(DstFd, DstOffset, Data);
		_ ->
		    {error, bad_crc}
	    end;
	{ok, _} ->
	    {error, truncated_record};
	Error ->
	    Error
    end.

%% Insert SrcSize data from SrcFd (cur) to DstFd at DstOffset
run_chunk_insert_file_(SrcFd, SrcSize, DstFd, DstOffset) ->
    {ok,SrcOffset} = file:position(SrcFd, cur),
    case exofile_lib:insert_data_from_file(DstFd,DstOffset,
					   SrcFd,SrcOffset,SrcSize) of
	ok -> %% fixme return crc32 over data!
	    %% fixme: check crc!
	    case file:position(SrcFd, SrcOffset+SrcSize+4) of
		{ok,_} -> ok;
		Error -> Error
	    end;
	Error ->
	    Error
    end.


%% SrcSize is small so first do a read and then fill
run_chunk_fill_bin_(SrcFd, SrcSize, DstFd, DstOffset, DstSize) ->
    case file:read(SrcFd, SrcSize+4) of
	{ok, <<Data:SrcSize/binary,CRC:32 >>} ->
	    case erlang:crc32(Data) of
		CRC ->
		    exofile_lib:fill_region(DstFd,DstOffset,Data,DstSize);
		_ ->
		    {error, bad_crc}
	    end;
	{ok, _} ->
	    {error, truncated_record};
	Error ->
	    Error
    end.

%% Move SrcSize data from SrcFd (cur) to DstFd at DstOffset
run_chunk_fill_file_(SrcFd, SrcSize, DstFd, DstOffset, DstSize) ->
    {ok,SrcOffset} = file:position(SrcFd, cur),
    case exofile_lib:fill_data_from_file(DstFd,DstOffset,DstSize,
					 SrcFd,SrcOffset,SrcSize) of
	ok -> %% fixme return crc32 over data!
	    %% fixme: check crc!
	    case file:position(SrcFd, SrcOffset+SrcSize+4) of
		{ok,_} -> ok;
		Error -> Error
	    end;
	Error ->
	    Error
    end.

%% SrcSize is small so first do a read and then fill
run_chunk_insfill_bin_(SrcFd, SrcSize, DstFd, DstOffset, DstSize) ->
    case file:read(SrcFd, SrcSize+4) of
	{ok, <<Data:SrcSize/binary,CRC:32 >>} ->
	    case erlang:crc32(Data) of
		CRC ->
		    exofile_lib:insfill_region(DstFd,DstOffset,Data,DstSize);
		_ ->
		    {error, bad_crc}
	    end;
	{ok, _} ->
	    {error, truncated_record};
	Error ->
	    Error
    end.

%% Move SrcSize data from SrcFd (cur) to DstFd at DstOffset
run_chunk_insfill_file_(SrcFd, SrcSize, DstFd, DstOffset, DstSize) ->
    {ok,SrcOffset} = file:position(SrcFd, cur),
    case exofile_lib:insfill_data_from_file(DstFd,DstOffset,DstSize,
					    SrcFd,SrcOffset,SrcSize) of
	ok -> %% fixme return crc32 over data!
	    %% fixme: check crc!
	    case file:position(SrcFd, SrcOffset+SrcSize+4) of
		{ok,_} -> ok;
		Error -> Error
	    end;
	Error ->
	    Error
    end.
