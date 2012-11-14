%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2012, Tony Rogvall
%%% @doc
%%%    Interval list handler
%%% @end
%%% Created :  7 Nov 2012 by Tony Rogvall <tony@rogvall.se>

-module(exofile_iset).

-export([new/0, from_list/1, to_list/1]).
-export([is_element/2, size/1, is_equal/2, is_subset/2, is_psubset/2]).
-export([first/1, last/1]).
-export([format/1]).

-export([offset/2, greater/2, less/2]).
-export([union/2, intersect/2, difference/2]).
-export([map/2,fold/3, foreach/2]).

-import(lists, [reverse/1, reverse/2]).

-type range() :: integer() | {integer(),integer()}.

%% domain is a sorted non overlapping list of ranges
-type domain() :: [range()].

%% @doc 
%%   Create a new empty iset
%% @end
-spec new() -> domain().

new() -> [].

%% @doc
%%   Create a domain from a list of ranges 
%% @end
-spec from_list([range()]) -> domain().

from_list(L) when is_list(L) ->
    from_list_(L).

from_list_([]) -> [];
from_list_([I]) when is_integer(I) -> [I];
from_list_([{L,L}]) when is_integer(L) -> [L];
from_list_([{L,H}]) when is_integer(L), is_integer(H), L<H -> [{L,H}];
from_list_(L=[_,_|_]) when is_list(L) ->
    N = length(L),
    {L1,L2} = lists:split(N div 2, L),
    A = from_list_(L1),
    B = from_list_(L2),
    union(A, B).

%% @doc
%%    Convert domain into a list
%% @end
-spec to_list(A::domain()) -> [integer()|{integer(),integer()}].
to_list(A) ->
    A.

%% @doc
%%   Check if N is a member in a domain
%% @end
-spec is_element(N::integer(), D::domain()) -> boolean().

is_element(N, [N | _]) -> true;
is_element(N, [X | _]) when is_integer(X), N < X -> false;
is_element(N, [X | L]) when is_integer(X), N > X -> is_element(N,L);
is_element(N, [{X0, X1} | _]) when N >= X0, N =< X1 -> true;
is_element(N, [{X0,_X1} | _]) when N < X0 -> false;
is_element(N, [{_X0,X1} | L]) when N  > X1 -> is_element(N, L);
is_element(_N, []) -> false.

%% @doc
%%   Calculate size of domain
%% @end
-spec size(A::domain()) -> non_neg_integer().
size(L) ->
    size(L, 0).

size([V | L], N) when is_integer(V) -> size(L, N+1);
size([{Low,High}|L], N) -> size(L, N+(High-Low)+1);
size([], N) -> N.

%% @doc
%%   Get minimum value from a non empty domain
%% @end
-spec first(D::domain()) -> integer().

first([{V,_} | _]) -> V;
first([V | _]) -> V.

%% @doc
%%   Get maximum value from a non empty domain
%% @end
-spec last(D::domain()) -> integer().
last(D) ->
    case reverse(D) of
	[{_,V} | _] -> V;
	[V | _]     -> V
    end.

%%
%% Fold
%%

fold(Fun,Acc,[D|Ds]) when is_integer(D) ->
    fold(Fun,Fun(D,Acc),Ds);
fold(Fun,Acc,[{L,H}|Ds]) ->
    fold_range(Fun,Acc,L,H,Ds);
fold(_Fun,Acc,[]) ->
    Acc.

fold_range(Fun,Acc,I,N,Ds) when I > N ->
    fold(Fun,Acc,Ds);
fold_range(Fun,Acc,I,N,Ds) ->
    fold_range(Fun,Fun(I,Acc),I+1,N,Ds).

%%
%% Map over iset
%%
map(_Fun, []) ->
    [];
map(Fun, [D|Ds]) when is_integer(D) ->
    [Fun(D) | map(Fun,Ds)];
map(Fun, [{L,H}|Ds]) ->
    map_range(Fun, L, H, Ds).

map_range(Fun, I, N, Ds) when I > N ->
    map(Fun, Ds);
map_range(Fun, I, N, Ds) ->
    [Fun(I) | map_range(Fun,I+1,N,Ds)].

%%
%% Foreach
%%
foreach(_Fun, []) ->
    true;
foreach(Fun, [D|Ds]) when is_integer(D) ->
    Fun(D),
    foreach(Fun,Ds);
foreach(Fun, [{L,H}|Ds]) ->
    foreach_range(Fun, L, H, Ds).

foreach_range(Fun, I, N, Ds) when I > N ->
    foreach(Fun, Ds);
foreach_range(Fun, I, N, Ds) ->
    Fun(I),
    foreach_range(Fun,I+1,N,Ds).

%% @doc
%%    Retrive the low value of the range
%% @end
-spec low(R::range()) -> integer() .

low({L,_}) -> L;
low(L) -> L.

%% @doc
%%    Retrive the high value of the range
%% @end
-spec high(R::range()) -> integer() .
high({_,H}) -> H;
high(H) -> H.

%% @doc
%%    Convert a range() to a pair 
%% @end
-spec pair(R::range()) -> {integer(),integer()}.
pair(R={_,_}) -> R;
pair(L) -> {L,L}.

%% @doc
%%    Create a range from low and high value
%% @end
-spec value(L::integer(),H::integer()) -> range().
value(N,N) -> N;
value(L,H) when L<H -> {L,H}.

%% @doc
%%    Check if two domains are equal
%% @end
-spec is_equal(A::domain(), B::domain()) -> boolean().

is_equal(A, A) -> true;
is_equal(_A, _B) -> false.

%% @doc
%%     Check if A is a subset of B
%% @end
-spec is_subset(A::domain(), B::domain()) -> boolean().
    
is_subset(A,B) ->
    case intersect(A, B) of
	A  -> true;
	_  -> false
    end.

%% @doc
%%     Check if A is a proper subset of B
%% @endf
is_psubset(A, B) ->
    case intersect(A, B) of
	A ->
	    case difference(B,A) of
		[] -> false;
		_  -> true
	    end;
	_ -> false
    end.

%% @doc
%%  Create a union of two domains
%% @end
-spec union(A::domain(), B::domain()) -> domain().

union(D,D) -> D;
union(D1,D2) ->
    union_(D1,D2,new()).

union_(D, [], U) -> 
    reverse(U,D);
union_([],D, U)  ->
    reverse(U,D);
union_(S1=[D1 | D1s], S2=[D2 | D2s], U) ->
    {Min1,Max1} = pair(D1),
    {Min2,Max2} = pair(D2),
    if Min2 == Max1+1 ->
	    union_(D1s,[value(Min1,Max2) | D2s], U);
       Min1 == Max2+1 ->
	    union_([value(Min2,Max1)|D1s], D2s, U);
       Min2 > Max1 ->
	    union_(D1s, S2, [D1|U]);
       Min1 > Max2 ->
	    union_(S1, D2s, [D2|U]);
       Max1 > Max2 ->
	    union_([value(min(Min1,Min2),Max1)|D1s], D2s, U);
       true ->
	    union_(D1s,[value(min(Min1,Min2),Max2)|D2s], U)
    end.

%% @doc
%%   Create the intersection between two domains
%% @end
-spec intersect(A::domain(), B::domain()) -> domain().

intersect([], _) -> [];
intersect(_, []) -> [];
intersect(D1, D2) ->
    intersect_(D1, D2, new()).

intersect_(_D, [], I) -> reverse(I);
intersect_([], _D, I) -> reverse(I);
intersect_(S1=[D1|D1s], S2=[D2|D2s], I) ->
    {Min1,Max1} = pair(D1),
    {Min2,Max2} = pair(D2),
    if Min2 > Max1 ->
 	    intersect_(D1s, S2, I);
       Min1 > Max2 ->
	    intersect_(S1, D2s, I);
       Max1<Max2 ->
	    intersect_(D1s,S2,
		       [value(max(Min1,Min2),Max1)|I]);
       true ->
	    intersect_(S1, D2s,
		       [value(max(Min1,Min2),Max2)|I])
    end.

%% @doc
%%   The difference between A and B, by removing the
%%   values in B from A i.e A - B
%% @end
-spec difference(A::domain(), B::domain()) -> domain().

difference(D, []) -> D;
difference([], _) -> [];
difference(D1, D2) ->
    difference_(D1,D2,new()).

difference_(D1s,[],Diff) -> 
    reverse(Diff,D1s);
difference_(D1s,[D2|D2s],Diff) ->
    Min2 = low(D2),
    Max2 = high(D2),
    ddiff(D1s, Min2, Max2, D2, D2s, Diff).

ddiff([], _Min2, _Max2, _D2, _D2s, Diff) ->
    reverse(Diff);
ddiff([D1 | D1s], Min2, Max2, D2, D2s, Diff) ->
    {Min1,Max1} = pair(D1),
    ddiffer(Min1, Max1, Min2, Max2, D1, D2, D1s,D2s,Diff).

ddiffer(Min1,Max1,Min2,Max2,D1,D2,D1s,D2s,Diff) ->
    if Min2 > Max1 ->
	    ddiff(D1s, Min2, Max2, D2, D2s, [D1 | Diff]);
       Min1 > Max2 ->
	    difference_([D1 | D1s], D2s, Diff);
       Min1<Min2 ->
	    D = value(Min1, Min2-1),
	    NewD1 = value(Min2, Max1),
	    ddiffer(Min2, Max1, Min2, Max2, 
		    NewD1, D2, D1s, D2s, [D|Diff]);
       Max1 > Max2 ->
	    NewD1 = value(Max2+1, Max1),
	    ddiffer(Max2+1, Max1, Min2, Max2, 
		    NewD1, D2, D1s, D2s, Diff);
       true ->
	    ddiff(D1s, Min2, Max2, D2, D2s, Diff)
    end.

%% @doc
%%   Offset a domain with a constant value
%% @end
-spec offset(Offset::integer(), D::domain()) -> domain().

offset(Offset, [{L,H}|D]) ->
    [{L+Offset,H+Offset}|offset(Offset, D)];
offset(Offset, [I|D]) ->
    [I+Offset|offset(Offset, D)];
offset(_Offset, []) ->
    [].

%% @doc
%%   Domain of values greater than V
%% @end
-spec greater(V::integer(), D::domain()) -> domain().

greater(V, [{_L,H}|D]) when H =< V -> greater(V, D);
greater(V, [I|D])      when I =< V -> greater(V, D);
greater(V, [{L,H}|D])  when V >= L, V < H -> 
    if H =:= V+1 -> [H|D];
       true -> [{V+1,H}|D]
    end;
greater(_V, D) ->
    D.

%% @doc
%%   Domain of values less than V
%% @end
-spec less(V::integer(), D::domain()) -> domain().

less(V, [{L,H}|D]) when H < V -> [{L,H}|less(V, D)];
less(V, [I|D])     when I < V -> [I|less(V, D)];
less(V, [{L,H}|_D]) when V > L, V =< H ->
    if V =:= L+1 -> [L];
       true -> [{L,V-1}]
    end;
less(_V, _D) ->
    [].

%% @doc
%%   Pretty format a domain
%% @end
-spec format(D::domain()) -> iolist().

format([]) -> "{}";
format([D]) -> ["{", format1(D), "}"];
format([D|Ds]) -> 
    ["{", format1(D), lists:map(fun(D1) -> [",",format1(D1)] end, Ds), "}"].

format1({Low,High}) -> [$[,integer_to_list(Low),"..",integer_to_list(High),$]];
format1(Value)      -> [integer_to_list(Value)].
