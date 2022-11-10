defmodule Postgresiar.Types.ElixirCode do
  use Utils

  use Ecto.Type

  ###########################################################################
  @doc """
    type: returns the native Ecto type of the field in the data storage, like :integer, :string, {:array, :string}…
  """
  @impl Ecto.Type
  def type, do: :string

  ###########################################################################
  @doc """
    cast: transforms the input data into the right format for the data storage. This function is called when you cast a struct into a changeset or when you pass arguments to an Ecto.Query.
    For example, if you need to store an Ecto.Date with different formats from the web forms (like “dd/mm/yyyy”), you can create a cast function for each format and when you create the changeset with
    the params, it will contain the Ecto.Date:
  """
  @impl Ecto.Type
  def cast(value) when is_bitstring(value) do
    {:ok, value}
  end

  @impl Ecto.Type
  def cast(value) when is_list(value) or (is_map(value) and not is_struct(value)) do
    {:ok, value}
  end

  @impl Ecto.Type
  def cast(value) when is_struct(value) do
    value = Map.from_struct(value)
    {:ok, inspect(value)}
  end

  def cast(_), do: :error

  ###########################################################################
  @doc """
    load: process raw data (the Ecto native type) when is read from the data storage and transform it to our custom type.
  """
  @impl Ecto.Type
  def load(value) when is_bitstring(value) do
    result = catch_error!(Utils.string_to_code!(value), false, false)

    value =
      case result do
        {:ok, value} ->
          value

        _ ->
          value
      end

    {:ok, value}
  end

  @impl Ecto.Type
  def load(_), do: :error

  ###########################################################################
  @doc """
    dump: validate the input data (casted data in the data struct) and transform it into the Ecto native type before is written to the data storage.
  """
  @impl Ecto.Type
  def dump(value) when is_bitstring(value) do
    {:ok, value}
  end

  @impl Ecto.Type
  def dump(value) when is_list(value) or (is_map(value) and not is_struct(value)) do
    {:ok, "#{inspect(value)}"}
  end

  @impl Ecto.Type
  def dump(value) when is_struct(value) do
    value = Map.from_struct(value)
    {:ok, "#{inspect(value)}"}
  end

  @impl Ecto.Type
  def dump(_), do: :error
end
