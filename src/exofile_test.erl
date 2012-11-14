%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2012, Tony Rogvall
%%% @doc
%%%    Test transfer
%%% @end
%%% Created :  7 Nov 2012 by Tony Rogvall <tony@rogvall.se>

-module(exofile_test).

-compile(export_all).

run() ->
    run("a.dat").

run(Name) ->
    exofile_srv:start_link(),
    transfer_file(filename:join(code:priv_dir(exofile), Name)).

transfer_file(File) ->
    transfer_file(File, filename:basename(File)).

transfer_file(File, Target) ->
    case file:open(File, [raw,read,binary]) of
	{ok,Fd} ->
	    FileSize = filelib:file_size(File),
	    case exofile:'transfer-init'(
		   [{'file-type', "text/plain"},
		    {'file-name-hint', Target},
		    {'file-size-hint', FileSize},
		    {'chunk-size-hint', 2048},
		    {'file-mode', read_write}]) of
		{ok, Reply} ->
		    ID = proplists:get_value('transfer-id',Reply),
		    Chunk = proplists:get_value('chunk-size',Reply,256),
		    SHA0  = crypto:sha_init(),
		    case transfer_data(Fd, ID, 0, SHA0, Chunk) of
			{ok,HexMac} ->
			    file:close(Fd),
			    exofile:'transfer-final'(
				   [{'transfer-id', ID},
				    {'file-size', FileSize},
				    {'file-name', Target},
				    {'sha1-digest', HexMac}]);
			Error ->
			    file:close(Fd),
			    Error
		    end
	    end;
	Error ->
	    Error
    end.

transfer_data(Fd, ID, Offset, SHA0, Chunk) ->
    case file:read(Fd, Chunk) of
	{ok, Data} ->
	    %% io:format("offset=~w, data=~w\n", [Offset, Data]),
	    case exofile:'chunk-write'([{'transfer-id', ID},
					{'offset', Offset},
					{'data', Data}]) of
		ok ->
		    SHA1 = crypto:sha_update(SHA0, Data),
		    transfer_data(Fd, ID, Offset+byte_size(Data), SHA1, Chunk);
		Error ->
		    Error
	    end;
	eof ->
	    Mac = crypto:sha_final(SHA0),
	    HexMac = bin2hex(Mac),
	    {ok,HexMac};
	Error ->
	    Error
    end.
		
bin2hex(Bin) ->
    [ element(I+1,{$0,$1,$2,$3,$4,$5,$6,$7,$8,$9,
		   $a,$b,$c,$d,$e,$f}) || <<I:4>> <= Bin].
