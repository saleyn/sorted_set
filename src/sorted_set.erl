%% @author Serge Aleynikov
%%
-module(sorted_set).
-on_load(init/0).

-export([new/0, new/1, new/2, from_list/1, from_list/2]).
-export([from_sorted_list/1, from_sorted_list/2, index_add/2, index_remove/2]).
-export([slice/3, find_index/2, size/1, at/2, at/3, add/2, remove/2, to_list/1]).
-export([default_capacity/0, default_bucket_size/0]).
-export([debug/1, jemalloc_allocation_info/0]).

-define(CAPACITY,    500).
-define(BUCKET_SIZE, 500).

-type sorted_set() :: reference().
%% `sorted_set's are stored in the NIF's memory space, constructing an operating on the `sorted_set' is
%% done through a reference that uniquely identifies the `sorted_set' in NIF space.

-type common_errors() :: {error, Reason::bad_reference|lock_fail|unsupported_type}.
%% There are common errors that can be returned from any `sorted_set' operation, the common_errors
%% type enumerates them.
%%
%% <b>Reason:</b>
%% <ul>
%% <li>`bad_reference' is returned any time a reference is passed to the NIF but that
%% reference does not identify a `sorted_set'.</li>
%% <li>`lock_fail' is returned when the NIF can not guarantee concurrency safety.
%% NIFs are not bound by the same guarantees as Erlang / Elixir code executing in the BEAM VM,
%% to safe guard against multiple threads of execution mutating the same `sorted_set' concurrently
%% a Mutex is used internally to lock the data structure during all operations.</li>
%% <li>`unsupported_type' is returned any time an item is passed to the `sorted_set'
%% that is either in whole or in part an unsupported type.  The following types are not supported
%% in `sorted_set', Reference, Function, Port, and Pid.  Unsupported types poison other types, so a
%% list containing a single element (regardless of nesting) of an unsupported type is unsupported,
%% same for tuples.
%% </ul>

-type nif_add_result() :: {ok, Result :: added|duplicate, Index::integer()}.
%% Success responses returned from the NIF when adding an element to the set.
%%
%% <b>Result:</b>
%% <ul>
%% <li>`added' is returned by the NIF to indicate that the add was executed
%% successfully and a new element was inserted into the `sorted_set' at the specified index.</li>
%% <li>`duplicate' is returned by the NIF to indicate that the add was executed successfully but
%% the element already existed within the `sorted_set', the index of the existing element is
%% returned.</li>
%% </ul>
%% The NIF provides more detailed but less conventional return values, these are coerced in the
%% `sorted_set' module to more conventional responses.  Due to how the NIF is implemented there is
%% no distinction in NIF space between `add' and `index_add', these more detailed response values
%% allow the Elixir wrapper to implement both with the same underlying mechanism.

-type nif_append_bucket_result() :: ok | {error, max_bucket_size_exceeded}.
%% Response returned from the NIF when appending a bucket.
%%
%% `ok' - is returned by the NIF to indicate that the bucket was appended.
%%
%% `{error, max_bucket_size_exceeded}' is returned by the NIF to indicate that the list of
%% terms passed in meets or exceeds the max_bucket_size of the set.

-type nif_at_result() :: {ok, Element :: any()} | {error, index_out_of_bounds}.
%% Response returned from the NIF when selecting an element at a given index
%% <ul>
%% <li>`{ok, element :: any()}' is returned by the NIF to indicate that the index was in
%% bounds and an element was found at the given index.</li>
%% <li>`{error, index_out_of_bounds}' is returned by the NIF to indicate that the index was not
%% within the bounds of the `sorted_set'.</li>
%% </ul>
%% The NIF provides more detailed by less conventional return values, these are coerced in
%% the `sorted_set' module to more conventional responses.  Specifically in the case of `at/3'
%% it is a common pattern to allow the caller to define a default value for when the element is
%% not found, there is no need to pay the penalty of copying this default value into and back
%% out of NIF space.

-type nif_find_result() :: {ok, Index :: integer()} | {error, not_found}.
%% Responses returned from the NIF when finding an element in the set.
%% <ul>
%% <li>`{ok, Index :: integer()}' is returned by the NIF to indicate that the element was
%% found at the specified index</li>
%% <li>`{error, not_found}' is returned by the NIF to indicate that the element was not found</li>
%% </ul>

-type nif_remove_result() :: {ok, removed, Index :: integer()} | {error, not_found}.
%% Responses returned from the NIF when removing an element in the set.
%% <ul>
%% <li>`{ok, removed, Index :: integer()}' is returned by the NIF to indicate that the remove
%% was executed successfully and the element has been removed from the set.  In addition it
%% returns the index that the element was found out prior to removal.</li>
%% <li>`{error, not_found}' is returned by the NIF to indicate that the remove was
%% executed successfully, but the specified element was not present in the `sorted_set'.</li>
%% </ul>
%% The NIF provides more detailed but less conventional return values, these are coerced in
%% the `sorted_set' module to more conventional responses.  Due to how the NIF is implemented
%% there is no distinction in NIF space between `remove` and `index_remove`, these more
%% detailed response values allow the Erlang wrapper to implement both with the same underlying
%% mechanism.

-type supported_term() :: integer() | atom() | tuple() | list() | binary().
%% Only a subset of Elixir types are supported by the nif, the semantic type `supported_term()'
%% can be used as a shorthand for terms of these supported types.

-export_type([sorted_set/0, common_errors/0, supported_term/0]).

%%------------------------------------------------------------------------------
%% External functions
%%------------------------------------------------------------------------------

%% @doc Construct a new `sorted_set()' with a given capacity and bucket size
%%
%% See the [README](/sorted_set_nif/doc/readme.html) for more information about how
%% `sorted_set()' works.
%% <dl>
%% <dt>Capacity</dt>
%% <dd>
%% The caller can pre-allocate capacity for the data structure, this can be helpful when the
%% initial set's size can be reasonably estimated.  The pre-allocation will be for the buckets but
%% not the contents of the buckets, so setting a high capacity and not using it is still memory
%% efficient.</dd>
%%
%% <dt>Bucket Size</dt>
%% <dd>
%% Internally the `sorted_set()' is a collection of sorted Buckets, this allows the `sorted_set()' to out
%% perform a simpler array of items.  The default bucket size was chosen based off of benchmarking
%% to select a size that performs well for most uses.</dd>
%%
%% <dt>Returned Resource</dt>
%% <dd>
%% Unlike a native Elixir data structure, the data in the `sorted_set()' is held in the NIF's memory
%% space, there are some important caveats to be aware of when using the `sorted_set()'.</dd>
%% </dl>
%%
%% First, `new/2' returns a `reference()'.  This `reference()` can be
%% used to access the `sorted_set' in subsequent calls.
%%
%% Second, because the data is stored in the NIF's memory space, the data structure acts more like
%% a mutable data structure than a standard immutable data structure.  It's best to treat the
%% `reference()' like one would treat an ETS `tid'.
new() ->
  new(?CAPACITY, ?BUCKET_SIZE).
new(Capacity) ->
  new(Capacity, ?BUCKET_SIZE).
new(Capacity, BucketSize) ->
  {ok, S} = new_nif(Capacity, BucketSize),
  S.

-spec from_list(list()) -> sorted_set() | common_errors().
from_list(L) ->
  from_list(L, ?BUCKET_SIZE).

%% @doc Construct a new `sorted_set()' from a list
%%
%% The list doesn't have to be sorted or unique.
from_list(L, BucketSize) when is_list(L), is_integer(BucketSize) ->
  L1 = sets:to_list(sets:from_list(L)),
  from_sorted_list(L1, BucketSize).

%% @doc Construct a new `sorted_set()' from a proper enumerable
%%
%% An enumerable is considered proper if it satisfies the following:
%% <ul>
%% <li>Enumerable is sorted</li>
%% <li>Enumerable contains no duplicates</li>
%% <li>Enumerable is made up entirely of supported terms</li>
%% </ul>
%% This method of construction is much faster than iterative construction.
%% See `from_list/2' for enumerables that are not proper.
-spec from_sorted_list(list()) -> sorted_set() | common_errors().
from_sorted_list(L) ->
  from_sorted_list(L, ?BUCKET_SIZE).
from_sorted_list(L, BucketSize) when is_list(L), is_integer(BucketSize) ->
  Ln = length(L),
  {ok, S0} = empty_nif(Ln, BucketSize),
  lists:foldl(fun(I, S) ->
                case append_bucket_nif(S, I) of
                  {ok, ok} ->
                    S;
                  {error, Reason} ->
                    throw(Reason)
                end
              end, S0, L).

%% @doc Adds an item to the set.
%%
%% To retrieve the index of where the item was added, see `index_add/2`  There is no performance
%% penalty for requesting the index while adding an item.
%%
%% <b>Performance</b>
%%
%% Unlike a hash based set that has O(1) inserts, the `sorted_set()' is O(log(N/B)) + O(log(B)) where
%% `N' is the number of items in the `sorted_set()' and `B' is the Bucket Size.
-spec add(Set :: sorted_set(), Item :: supported_term()) ->
        sorted_set() | common_errors().
add(Set, Item) ->
  case add_nif(Set, Item) of
    {ok, _} ->
      Set;
    Other ->
      Other
  end.

%% @doc Adds an item to the set, returning the index.
%%
%% If the index is not needed the `add/2' function can be used instead, there is no performance
%% difference between these two functions.
%%
%% <b>Performance</b>
%%
%% Unlike a hash based set that has O(1) inserts, the `sorted_set()' is `O(log(N/B)) +
%% O(log(B))' where `N' is the number of items in the `sorted_set()' and `B' is the Bucket Size.
-spec index_add(Set :: sorted_set(), Item :: any()) ->
        {Index :: non_neg_integer() | nil, sorted_set()} | common_errors().
index_add(Set, Item) ->
  case add_nif(Set, Item) of
    {ok, {added, Index}} ->
      {Index, Set};
    {ok, {duplicate, _}} ->
      {nil, Set};
    Other ->
      Other
  end.

%% @doc Removes an item from the set.
%%
%% If the item is not present in the set, the set is simply returned.  To retrieve the index of
%% where the item was removed from, see `index_remove/2'.  There is no performance penalty for
%% requesting the index while removing an item.
%%
%% <b>Performance</b>
%%
%% Unlike a hash based set that has O(1) removes, the `sorted_set()' is O(log(N/B)) + O(log(B)) where
%% `N' is the number of items in the `sorted_set()' and `B' is the Bucket Size.
-spec remove(Set :: sorted_set(), Item :: any()) ->
        sorted_set() | common_errors().
remove(Set, Item) ->
  case remove_nif(Set, Item) of
    {ok, {removed, _}} ->
      Set;
    {error, not_found} ->
      Set;
    Other ->
      Other
  end.

%% @doc Removes an item from the set, returning the index of the item before removal.
%%
%% If the item is not present in the set, the index `nil' is returned along with the set.  If the
%% index is not needed the `remove/2' function can be used instead, there is no performance
%% difference between these two functions.
%%
%% <b>Performance</b>
%%
%% Unlike a hash based set that has O(1) removes, the `sorted_set()' is
%% `O(log(N/B)) + O(log(B))' where `N' is the number of items in the
%% `sorted_set()' and `B' is the Bucket Size.
-spec index_remove(Set :: sorted_set(), Item :: any()) ->
          {Index :: non_neg_integer(), sorted_set()} | common_errors().
index_remove(Set, Item) ->
  case remove_nif(Set, Item) of
    {ok, {removed, Index}} ->
      {Index, Set};
    {error, not_found} ->
      {nil, Set};
    Other ->
      Other
  end.

%% @doc Get the size of a `sorted_set()'
%%
%% This function follows the standard Elixir naming convention, `size/1` take O(1) time as
%% the size is tracked with every addition and removal.
-spec size(Set :: sorted_set()) ->
        non_neg_integer() | common_errors().
size(Set) ->
  case size_nif(Set) of
    {ok, Size} -> Size;
    Other      -> Other
  end.

%% @doc Converts a `sorted_set()' into a List
%%
%% This operation requires copying the entire `sorted_set()' out of NIF space and back into
%% Elixir space it can be very expensive.
-spec to_list(Set :: sorted_set()) ->
        [supported_term()] | common_errors().
to_list(Set) ->
  case to_list_nif(Set) of
    {ok, Result} when is_list(Result) ->
      Result;
    Other ->
      Other
  end.

%% @doc Retrieve an item at the given index.
%%
%% If the index is out of bounds then the optional default value is returned instead, this defaults
%% to `nil' if not provided.
-spec at(Set :: sorted_set(), Index :: non_neg_integer(), Default :: any()) ->
          (ItemOrDefault :: supported_term() | any()) | common_errors().
at(Set, Index, Default) ->
  case at_nif(Set, Index) of
    {ok, Item} ->
      Item;
    {error, index_out_of_bounds} ->
      Default;
    {error, _} = Other ->
      Other
  end.

at(Set, Index) ->
  at(Set, Index, nil).

%% @doc Retrieves a slice of the `sorted_set()' starting at the specified index and including up
%% to the specified amount.
%%
%% `split/3' will return an empty list if the start index is out of bounds.  If the `Amount'
%% exceeds the number of items from the start index to the end of the set then all terms up to the
%% end of the set will be returned.  This means that the length of the list returned by slice will
%% fall into the range of [0, `amount']
-spec slice(Set :: sorted_set(), Start :: non_neg_integer(), Amount :: non_neg_integer()) ->
        [supported_term()] | common_errors().
slice(Set, Start, Amount) ->
  case slice_nif(Set, Start, Amount) of
    {ok, Items} when is_list(Items) ->
      Items;
    Other ->
      Other
  end.

%% @doc Finds the index of the specified term.
%%
%% Since `sorted_set()' does enforce uniqueness of terms there is no need to worry about which index
%% gets returned, the term either exists in the set or does not exist in the set.  If the term
%% exists the index of the term is returned, if not then `nil` is returned.
-spec find_index(Set :: sorted_set(), Item :: supported_term()) ->
        non_neg_integer() | nil | common_errors().
find_index(Set, Item) ->
  case find_index_nif(Set, Item) of
    {ok, Index} ->
      Index;
    {error, not_found} ->
      nil;
    Other ->
      Other
  end.

%% @doc Helper function to access the `default_capacity' module attribute
-spec default_capacity()    -> pos_integer().
default_capacity()          -> ?CAPACITY.

%% @doc Helper function to access the `default_bucket_size' module attribute
-spec default_bucket_size() -> pos_integer().
default_bucket_size()       -> ?BUCKET_SIZE.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% @doc %% Creates a new `sorted_set'.
%%
%% To prevent copying the set into and out of NIF space, the NIF returns an opaque
%% reference handle that should be used in all subsequent calls to identify the
%% `sorted_set'.
-spec new_nif(Capacity :: pos_integer(), BucketSize :: pos_integer()) ->
        {ok, sorted_set()}.
new_nif(_Capacity, _Bucket_size) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Creates an empty `sorted_set'.
%%
%% This is mostly an internal implementation detail, it is used to implement the
%% `sorted_set:from_enumerable/2' and `sorted_set:from_proper_enumerable/2'
%% functions.  The only valid operation that can be performed on an empty `sorted_set'
%% is `append_bucket/2', all other functions expect that the bucket not be completely
%% empty.
-spec empty_nif(Capacity::pos_integer(), BucketSize::pos_integer()) ->
        {ok, sorted_set()}.
empty_nif(_Capacity, _BucketSize) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Appends a buckets worth of sorted terms to the `sorted_set'
%
%% This is mostly an internal implementation detail, it is used to implement the
%% `sorted_set:from_enumerable/2' and `sorted_set:from_proper_enumerable/2'
%% functions.  The NIF will append a buckets worth of items without performing any checks on them.
%% This is a very efficient way to build the `sorted_set' but care must be taken since the call
%% circumvents the sorting and sanity checking logic.  Use the constructors in `sorted_set'
%% for a safer and more ergonomic experience, use great care when calling this function directly.
-spec append_bucket_nif(Set::sorted_set(), Terms::[supported_term()]) ->
          ok | nif_append_bucket_result() | common_errors().
append_bucket_nif(_Set, _Terms) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Gets the size of the `sorted_set'.
%% This function follows the standard Elixir naming convention, size takes O(1) time as the
%% size is tracked with every addition and removal.
-spec size_nif(Set :: sorted_set()) -> non_neg_integer().
size_nif(_Set) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Adds an item to the `sorted_set'.
%% This function follows the standard Elixir naming convention, size takes O(1) time as the
%% size is tracked with every addition and removal.
-spec add_nif(Set :: sorted_set(), Item :: any()) ->
        nif_add_result() | common_errors().
add_nif(_Set, _Item) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Removes an item from the `sorted_set'.
-spec remove_nif(Set::sorted_set(), Item::any()) ->
        nif_remove_result() | common_errors().
remove_nif(_Set, _Item) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Retrieve the item at the specified index
-spec at_nif(Set :: sorted_set(), Index :: non_neg_integer()) ->
         nif_at_result() | common_errors().
at_nif(_Set, _Index) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Retrieve a slice of starting at the start index and taking up to amount
-spec slice_nif(Set :: sorted_set(), Start :: non_neg_integer(), Amount :: non_neg_integer()) ->
        [any()] | common_errors().
slice_nif(_Set, _Start, _Amount) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Finds the index of the specified item
-spec find_index_nif(Set :: sorted_set(), Item :: any()) ->
        nif_find_result() | common_errors().
find_index_nif(_Set, _Item) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Converts a `sorted_set' into a standard list
%% Note: This is potentially an expensive operation because it must copy the NIF data
%% back into BEAM VM space.
-spec to_list_nif(Set :: sorted_set()) -> [any()] | common_errors().
to_list_nif(_Set) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Returns a string representation of the underlying Rust data structure.
%% This function is mostly provided as a convenience, since the actual data structure is
%% stored in the NIF memory space it can be difficult to introspect the data structure as
%% it changes.  This function allows the caller to get the view of the data structure as
%% Rust sees it.
-spec debug(Set :: sorted_set()) -> binary() | common_errors().
debug(_Set) ->
  erlang:nif_error(nif_not_loaded).

%% @doc Returns information about this NIF's memory allocations, as reported by jemalloc.
jemalloc_allocation_info() ->
  erlang:nif_error(nif_not_loaded).

-define(LIBNAME, "libsorted_set_erl").

init() ->
  SoName = getdir(?LIBNAME),
  erlang:load_nif(SoName, 0).

getdir(LibName) ->
  case code:priv_dir(sorted_set) of
    {error, bad_name} ->
      case code:which(?MODULE) of
        Filename when is_list(Filename) ->
          filename:join([filename:dirname(filename:dirname(Filename)), "priv", LibName]);
        _ ->
          filename:join("../priv", LibName)
      end;
    Dir ->
      filename:join(Dir, LibName)
  end.
