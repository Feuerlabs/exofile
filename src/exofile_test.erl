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
    case file:open(File, [read, binary]) of
	{ok,Fd} ->
	    case exofile:'transfer-init'(
		   [{'file-type', "text/plain"},
		    {'file-name-hint', File},
		    {'chunk-size-hint', 2048},
		    {'file-mode', read_write}]) of
		{ok, Reply} ->
		    ID = proplists:get_value('transfer-id',Reply),
		    Chunk = proplists:get_value('chunk-size',Reply,256),
		    SHA  = crypto:sha_init(),
		    io:format("ID=~s, Chunk=~w\n", [ID,Chunk]),
		    Result1 = transfer_data(Fd, ID, 0, SHA, Chunk),
		    file:close(Fd),
		    Result1;
		Error ->
		    Error
	    end;
	Error ->
	    Error
    end.

transfer_data(Fd, ID, Offset, SHA, Chunk) ->
    case file:read(Fd, Chunk) of
	{ok, Data} ->
	    io:format("offset=~w, data=~w\n", [Offset, Data]),
	    case exofile:'transfer-chunk-write'([{'transfer-id', ID},
						 {'offset', Offset},
						 {'data', Data}]) of
		ok ->
		    SHA1 = crypto:sha_update(SHA, Data),
		    transfer_data(Fd, ID, Offset+byte_size(Data), SHA1, Chunk);
		Error ->
		    Error
	    end;
	eof ->
	    Mac = crypto:sha_final(SHA),
	    Hex = bin2hex(Mac),
	    case exofile:'transfer-final'([{'transfer-id', ID},
					   {'file-size', Offset},
					   {'file-name', "hello.dat"},
					   {'sha1-digest', Hex}]) of
		ok ->
		    file:close(Fd),
		    ok;
		Error ->
		    file:close(Fd),
		    Error
	    end
    end.

bin2hex(Bin) ->
    [ element(I+1,{$0,$1,$2,$3,$4,$5,$6,$7,$8,$9,
		   $a,$b,$c,$d,$e,$f}) || <<I:4>> <= Bin].
