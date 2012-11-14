%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2012, Tony Rogvall
%%% @doc
%%%     exofile operations
%%% @end
%%% Created :  6 Nov 2012 by Tony Rogvall <tony@rogvall.se>

-module(exofile).

-export(['transfer-list'/1,
	 'transfer-init'/1,
	 'transfer-final'/1,
	 'transfer-cancel'/1,
	 'chunk-write'/1,
	 'chunk-read'/1,
	 'chunk-insert'/1,
	 'chunk-delete'/1,
	 'chunk-fill'/1,
	 'chunk-insfill'/1,
	 'chunk-list-transfered'/1,
	 'chunk-list-missing'/1
	]).

-export([start/0, stop/0]).

-define(SERVER, exofile_srv).

%% testing only (use application:start(exofile))
start() ->
    exofile_srv:start_link().

stop() ->
    exofile_srv:stop().

%%
%% List all ongoing file transfers
%%
'transfer-list'(Args)  when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'transfer-list',
		     proplists:get_value('n', Args, 5),
		     proplists:get_value('previous', Args, "")
		    }).

%%
%% Cancel an ongoing file transfers
%%
'transfer-cancel'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'transfer-cancel',
		     proplists:get_value('transfer-id', Args)
		    }).

%%
%% Initiate a new file transfers
%%
'transfer-init'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'transfer-init',
		     proplists:get_value('file-size-hint', Args, 0),
		     proplists:get_value('chunk-size-hint', Args, 0),
		     proplists:get_value('file-type', Args, ""),
		     proplists:get_value('file-name-hint', Args, ""),
		     proplists:get_value('file-mode', Args, write)
		    }).

%%
%% Terminate an ongoing file transfer, normal
%%
'transfer-final'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'transfer-final',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('file-size', Args),
		     proplists:get_value('file-name', Args),
		     proplists:get_value('sha1-digest', Args)
		    }).



%%
%% Transfer a single file chunk
%%
'chunk-write'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'chunk-write',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('offset', Args),
		     proplists:get_value('data', Args)}).

'chunk-read'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'chunk-read',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('offset', Args),
		     proplists:get_value('size', Args)}).

'chunk-insert'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'chunk-insert',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('offset', Args),
		     proplists:get_value('data', Args)}).

'chunk-delete'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'chunk-delete',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('offset', Args),
		     proplists:get_value('size', Args)}).

'chunk-fill'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'chunk-fill',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('offset', Args),
		     proplists:get_value('size', Args),
		     proplists:get_value('data', Args)}).

'chunk-insfill'(Args) when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'transfer-chunk-insfill',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('offset', Args),
		     proplists:get_value('size', Args),
		     proplists:get_value('data', Args)}).

'chunk-list-transfered'(Args)  when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'chunk-list-transfered',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('n', Args, 5),
		     proplists:get_value('offset', Args, 0)
		    }).

'chunk-list-missing'(Args)  when is_list(Args) ->
    gen_server:call(?SERVER, 
		    {'chunk-list-missing',
		     proplists:get_value('transfer-id', Args),
		     proplists:get_value('n', Args, 5),
		     proplists:get_value('offset', Args, 0),
		     proplists:get_value('file-size', Args, 0)
		    }).
