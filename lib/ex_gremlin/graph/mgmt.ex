defmodule ExGremlin.Mgmt do
  @moduledoc false

  alias :queue, as: Queue

  @type t :: {[], []}

  @doc """
  Start of graph management. All operations are stored in a queue.
  """

  @spec new :: ExGremlin.Mgmt.t()
  def new, do: Queue.new()

  def open_management(management) do
    Queue.in(_open_management(),management)
  end
  def open_management() do
    Queue.in(_open_management(),new())
  end

  def add_query(management, query) do
  	Queue.in(query, management)
  end

  def commit(management) do
  	Queue.in(_commit(),management)
  end

  def tx_commit(management) do
    Queue.in(_graph_tx_commit(),management)
  end

  def commit_all_open_transactions(management) do
    Queue.in(_commit_all_open_transactions(),management)
  end

  def rollback_all_open_transactions(management) do
    Queue.in(_rollback_all_open_transactions(),management)
  end

  @doc """
  Compiles a graph into the Gremlin query.
  """
  @spec encode(ExGremlin.Management.t()) :: String.t()
  def encode(management) do
    encode(management, "")
  end

  defp encode({[],[]}, acc), do: acc

  defp encode(management, acc) do
    {{:value, query}, remainder} = :queue.out(management)

    # encode(remainder, acc <> escape(query))
    encode(remainder, acc <> query)
  end

  # @spec escape(String.t()) :: String.t()
  # defp escape(str) do
  #   # We escape single quote if it is not already escaped by an odd number of backslashes
  #   String.replace(str, ~r/((\A|[^\\])(\\\\)*)'/, "\\1\\'")
  # end

  defp _open_management() do
	"""
	mgmt = graph.openManagement();
	"""  	
  end
		
	def _commit() do
		"""
		mgmt.commit();
		"""
	end

	def _rollback() do
		"""
		mgmt.rollback();
		"""
	end

	def _graph_tx_commit() do
		"""
		graph.tx().commit();
		"""
	end
	def _commit_all_open_transactions() do
		"""
		cmot_size = graph.getOpenTransactions().size;
		if(cmot_size > 0){
			for(i = 0; i < cm_size; i++){
				graph.getOpenTransactions().getAt(0).commit();
			}
		}
		"""
	end
	def _rollback_all_open_transactions() do
		"""
		rbot_size = graph.getOpenTransactions().size
		if(rbot_size > 0){
			for(i = 0; i < rb_size; i++){
				graph.getOpenTransactions().getAt(0).rollback();
			}
		}
		"""			
	end

  defprotocol Query do

    #------------ property key manamgement
    @spec get_property_key(MgmtQuery.t()) :: String.t()
    def get_property_key(key_info)

    @spec make_property_key(MgmtQuery.t()) :: String.t()
    def make_property_key(key_info)

    #------------ vertex management
    @spec get_vertex_label(MgmtQuery.t()) :: String.t()
    def get_vertex_label(vertex_info)

    @spec make_vertex_label(MgmtQuery.t()) :: String.t()
    def make_vertex_label(vertex_info)
    #------------ edge management
    @spec get_edge_label(MgmtQuery.t()) :: String.t()
    def get_edge_label(edge_info)

    @spec make_edge_label(MgmtQuery.t()) :: String.t()
    def make_edge_label(edge_info)

  end

end