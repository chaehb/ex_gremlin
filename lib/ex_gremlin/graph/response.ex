defmodule ExGremlin.Response do
  @moduledoc """
  originally made by Gremlex  
  """
  require Logger

  # @status_success           200
  # @status_no_content          204
  # @status_partial_content       206
  # @status_unauthorized              401
  # @status_authenticate              407
  # @status_malformed_request         498
  # @status_invalid_request_arguments 499
  # @status_server_error              500
  # @status_script_evaluation_error     597
  # @status_server_timeout            598
  # @status_server_serialization_error  599

  # def gremlin_status(@status_success), do: :success
  # def gremlin_status(@status_no_content), do: :no_content
  # def gremlin_status(@status_partial_content), do: :partial_content
  # def gremlin_status(@status_unauthorized), do: :unauthorized
  # def gremlin_status(@status_authenticate), do: :authenticate
  # def gremlin_status(@status_malformed_request), do: :malformed_request
  # def gremlin_status(@status_invalid_request_arguments), do: :invalid_request_arguments
  # def gremlin_status(@status_server_error), do: :server_error
  # def gremlin_status(@status_script_evaluation_error), do: :script_evaluation_error
  # def gremlin_status(@status_server_timeout), do: :server_timeout
  # def gremlin_status(@status_server_serialization_error), do: :server_serialization_error
  
  # def gremlin_status(:success), do: @status_success
  # def gremlin_status(:no_content), do: @status_no_content
  # def gremlin_status(:partial_content), do: @status_partial_content
  # def gremlin_status(:unauthorized), do: @status_unauthorized
  # def gremlin_status(:authenticate), do: @status_authenticate
  # def gremlin_status(:malformed_request), do: @status_malformed_request
  # def gremlin_status(:invalid_request_arguments), do: @status_invalid_request_arguments
  # def gremlin_status(:server_error), do: @status_server_error
  # def gremlin_status(:script_evaluation_error), do: @status_script_evaluation_error
  # def gremlin_status(:server_timeout), do: @status_server_timeout
  # def gremlin_status(:server_serialization_error), do: @status_server_serialization_error

  # def gremlin_status(code), do: code

  def parse(data) do
    response = Jason.decode!(data)
    result = ExGremlin.Deserializer.deserialize(response)
    status = response["status"]["code"]
    error_message = response["status"]["message"]

    case status do
        200 -> # status_success
          {:ok, result}
        204 -> # status_no_content
          {:ok, []}
        # 206 -> # status_partial_content
        #   recv(socket, result)
        401 -> # unauthorized
          {:error, {:unauthorized, error_message}}

        409 -> # malformed_request
          {:error, {:malformed_request, error_message}}

        499 -> # invalid_request_arguments
          {:error, {:invalid_request_arguments, error_message}}

        500 -> # server_error
          {:error, {:server_error, error_message}}

        597 -> # script_evaluation_error
          {:error, {:script_evaluation_error, error_message}}

        598 -> # server_timeout
          {:error, {:server_timeout, error_message}}

        599 -> # server_serialization_error
          {:error, {:server_serialization_error, error_message}}
        code ->
          Logger.debug "response [#{code}] : #{error_message}"
          {:error, {code, error_message}}
    end
  end
end