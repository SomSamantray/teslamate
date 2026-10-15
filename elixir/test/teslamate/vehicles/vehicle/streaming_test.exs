defmodule TeslaMate.Vehicles.Vehicle.StreamingTest do
  use TeslaMate.VehicleCase, async: false

  import ExUnit.CaptureLog

  alias TeslaMate.Locations.GeoFence
  alias TeslaMate.Vehicles.Vehicle.Summary
  alias TeslaMate.Vehicles.Vehicle
  alias TeslaApi.Stream

  defp stream(name, data) do
    base_attrs = %{
      shift_state: nil,
      est_lat: 42.10,
      est_lng: 42.00,
      speed: 0,
      odometer: 1000,
      elevation: 10,
      est_heading: 120,
      est_range: 200,
      range: 180,
      heading: 300,
      power: 0,
      soc: 60,
      time: nil
    }

    send(name, {:stream, struct!(Stream.Data, Map.merge(base_attrs, data))})
  end

  describe "driving" do
    @log_opts format: "[$level] $message
",
              colors: [enabled: false]

    @tag :capture_log
    test "starts a drive", %{test: name} do
      me = self()
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)

      events = [
        {:ok, online_event(now_ts)},
        {:ok, online_event(now_ts, drive_state: %{timestamp: now_ts})},
        fn ->
          send(me, :continue?)

          receive do
            :continue -> {:error, :closed}
          after
            5_000 -> raise "No :continue after 5s"
          end
        end,
        {:error, :closed},
        {:error, :closed},
        {:error, :closed},
        {:error, :closed},
        {:ok,
         online_event(now_ts + 10,
           drive_state: %{
             timestamp: now_ts + 10,
             latitude: 42.91,
             longitude: 42.81,
             shift_state: "P",
             speed: 0,
             power: 0
           }
         )},
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_vehicle(name, events)

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      assert_receive :continue?
      stream(name, %{shift_state: "P", time: now})

      # Every frame is an observation for the summary, also a parked one (#5863)
      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{state: :online, shift_state: "P", speed: 0, elevation: 10}}}

      refute_receive _

      stream(name, %{shift_state: "D", speed: 10, power: 5, elevation: 1, time: now})
      assert_receive {:start_drive, ^car}
      assert_receive {:insert_position, drive, position}

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{state: :driving, speed: 16, power: 5, elevation: 1}}}

      assert position == %{
               latitude: 42.1,
               longitude: 42.0,
               speed: 16,
               battery_level: 60,
               date: now,
               elevation: 1,
               odometer: 1609.344,
               power: 5
             }

      stream(name, %{
        shift_state: "D",
        speed: 15,
        power: 10,
        est_lat: 42.31,
        elevation: 2,
        time: now
      })

      assert_receive {:insert_position, ^drive,
                      %{speed: 24, power: 10, latitude: 42.31, elevation: 2}}

      assert_receive {:pubsub,
                      {:broadcast, _, _, %Summary{state: :driving, speed: 24, power: 10}}}

      stream(name, %{
        shift_state: "N",
        speed: 20,
        power: 2,
        est_lat: 42.32,
        elevation: 3,
        time: now
      })

      assert_receive {:insert_position, ^drive,
                      %{speed: 32, power: 2, latitude: 42.32, elevation: 3}}

      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :driving, speed: 32, power: 2}}}

      stream(name, %{
        shift_state: "R",
        speed: 3,
        power: 1,
        est_lat: 42.33,
        elevation: 4,
        time: now
      })

      assert_receive {:insert_position, ^drive,
                      %{speed: 5, power: 1, latitude: 42.33, elevation: 4}}

      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :driving, speed: 5, power: 1}}}

      send(:"api_#{name}", :continue)
      stream(name, %{shift_state: "P", speed: nil, power: nil, elevation: 5, time: now})

      # A stationary, parked frame shows no speed; its power was not
      # transmitted and keeps the previous value (#5863, criterion 4)
      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{
                         state: :driving,
                         shift_state: "P",
                         speed: nil,
                         power: 1,
                         elevation: 5
                       }}}

      assert_receive {:insert_position, ^drive, %{speed: 0, power: 0}}
      assert_receive {:close_drive, ^drive, lookup_address: true}

      assert_receive {:start_state, ^car, :online, date: _}

      assert_receive {:insert_position, ^car, %{latitude: 42.91, longitude: 42.81, speed: 0}}

      # The newer poll shows its own values; the elevation is the newest one
      # the stream measured
      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{
                         state: :online,
                         latitude: 42.91,
                         longitude: 42.81,
                         shift_state: "P",
                         speed: 0,
                         power: 0,
                         elevation: 5
                       }}}

      refute_receive _
    end

    test "updates the geofence from streaming positions", %{test: name} do
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)

      events = [
        {:ok,
         online_event(now_ts,
           drive_state: %{timestamp: now_ts, latitude: 90, longitude: 45}
         )},
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_vehicle(name, events)

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{latitude: 90, longitude: 45}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{state: :online, geofence: %GeoFence{name: "South Pole"}}}}

      stream(name, %{shift_state: "D", est_lat: 90, est_lng: 45, time: now})
      assert_receive {:start_drive, ^car}
      assert_receive {:insert_position, drive, %{latitude: 90, longitude: 45}}

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{state: :driving, geofence: %GeoFence{name: "South Pole"}}}}

      stream(name, %{shift_state: "D", est_lat: 90, est_lng: 45.01, time: now})
      assert_receive {:insert_position, ^drive, %{latitude: 90, longitude: 45.01}}

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{state: :driving, geofence: %GeoFence{name: "Garage"}}}}

      stream(name, %{shift_state: "D", est_lat: 90, est_lng: 45.1, time: now})
      assert_receive {:insert_position, ^drive, %{latitude: 90, longitude: 45.1}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :driving, geofence: nil}}}

      stream(name, %{shift_state: "D", est_lat: 90, est_lng: 45, time: now})
      assert_receive {:insert_position, ^drive, %{latitude: 90, longitude: 45}}

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{state: :driving, geofence: %GeoFence{name: "South Pole"}}}}
    end

    test "discards fetch result", %{test: name} do
      me = self()
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)

      events = [
        {:ok, online_event(now_ts)},
        fn ->
          send(me, :continue?)

          receive do
            :continue ->
              {:ok,
               online_event(now_ts + 0,
                 drive_state: %{
                   timestamp: now_ts + 0,
                   latitude: 42.91,
                   longitude: 42.81,
                   shift_state: "P",
                   speed: 0,
                   power: 0
                 }
               )}
          after
            5_000 -> raise "No :continue after 5s"
          end
        end,
        fn ->
          send(me, :continue?)

          receive do
            :continue ->
              {:ok,
               online_event(now_ts + 4,
                 drive_state: %{
                   timestamp: now_ts + 4,
                   latitude: 42.91,
                   longitude: 42.81,
                   shift_state: "P",
                   speed: 0,
                   power: 0
                 }
               )}
          after
            5_000 -> raise "No :continue after 5s"
          end
        end,
        fn -> Process.sleep(10_000) end
      ]

      d1 = DateTime.from_unix!(now_ts + 1, :millisecond)
      d2 = DateTime.from_unix!(now_ts + 2, :millisecond)
      d3 = DateTime.from_unix!(now_ts + 2, :millisecond)
      d4 = DateTime.from_unix!(now_ts + 4, :millisecond)

      :ok = start_vehicle(name, events)

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      stream(name, %{shift_state: "D", time: d1})
      assert_receive {:start_drive, ^car}
      assert_receive {:insert_position, drive, %{date: ^d1}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :driving, elevation: 10}}}

      stream(name, %{shift_state: "D", time: d2})
      assert_receive {:insert_position, ^drive, %{date: ^d2}}

      assert_receive :continue?
      refute_receive _

      assert capture_log(@log_opts, fn ->
               stream(name, %{shift_state: "P", speed: nil, power: nil, time: d3})

               # The parked frame is shown at once (#5863)
               assert_receive {:pubsub,
                               {:broadcast, _, _,
                                %Summary{state: :driving, shift_state: "P", speed: nil, power: 0}}}

               send(:"api_#{name}", :continue)

               assert_receive :continue?
               refute_receive _
             end) =~ """
             Discarded stale fetch result: [
               now: %TeslaApi.Vehicle.State.Drive{
                 active_route_destination: nil,
                 active_route_energy_at_arrival: nil,
                 active_route_latitude: nil,
                 active_route_longitude: nil,
                 active_route_miles_to_arrival: nil,
                 active_route_minutes_to_arrival: nil,
                 active_route_traffic_minutes_delay: nil,
                 gps_as_of: nil,
                 heading: nil,
                 latitude: 42.91,
                 longitude: 42.81,
                 native_latitude: nil,
                 native_location_supported: nil,
                 native_longitude: nil,
                 native_type: nil,
                 power: 0,
                 shift_state: \"P\",
                 speed: 0,
                 timestamp: #{now_ts}
               },
               last: %TeslaApi.Vehicle.State.Drive{
                 active_route_destination: nil,
                 active_route_energy_at_arrival: nil,
                 active_route_latitude: nil,
                 active_route_longitude: nil,
                 active_route_miles_to_arrival: nil,
                 active_route_minutes_to_arrival: nil,
                 active_route_traffic_minutes_delay: nil,
                 gps_as_of: nil,
                 heading: 120,
                 latitude: 42.1,
                 longitude: 42.0,
                 native_latitude: nil,
                 native_location_supported: nil,
                 native_longitude: nil,
                 native_type: nil,
                 power: 0,
                 shift_state: \"D\",
                 speed: 0,
                 timestamp: #{DateTime.to_unix(d1, :millisecond)}
               }
             ]
             """

      send(:"api_#{name}", :continue)

      assert_receive {:insert_position, ^drive, %{date: ^d4, speed: 0, power: 0}}
      assert_receive {:close_drive, ^drive, lookup_address: true}

      assert_receive {:start_state, ^car, :online, date: ^d4}
      assert_receive {:insert_position, ^car, %{date: ^d4}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      refute_receive _
    end

    @tag :capture_log
    test "discards stale stream data", %{test: name} do
      now = DateTime.utc_now()
      now_ts = now |> DateTime.to_unix(:millisecond)

      events = [
        {:ok, online_event(now_ts)},
        {:ok, online_event(now_ts, drive_state: %{timestamp: now_ts})},
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_vehicle(name, events)

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      refute_receive _

      assert capture_log(@log_opts, fn ->
               stream(name, %{shift_state: "P", time: DateTime.add(now, -1, :second)})

               # The older frame loses against the poll's drive state; only its
               # elevation, which no poll measures, is shown (#5863)
               assert_receive {:pubsub,
                               {:broadcast, _, _,
                                %Summary{state: :online, shift_state: nil, elevation: 10}}}

               refute_receive _
             end) =~ "Received stale stream data"
    end
  end

  describe "charging" do
    @tag :capture_log
    test "starts charging", %{test: name} do
      me = self()
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)

      events = [
        {:ok, online_event(now_ts)},
        {:ok, online_event(now_ts, drive_state: %{timestamp: now_ts})},
        fn ->
          send(me, :continue?)

          receive do
            :continue -> {:error, :closed}
          after
            5_000 -> raise "No :continue after 5s"
          end
        end,
        {:ok, charging_event(now_ts + 1, "Charging", 0.0)},
        {:ok, charging_event(now_ts + 2, "Charging", 1.0)},
        {:ok, charging_event(now_ts + 3, "Stopped", 1.1)},
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_vehicle(name, events)

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      assert_receive :continue?
      stream(name, %{shift_state: nil, power: -1, time: now})
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online, power: -1}}}
      send(:"api_#{name}", :continue)

      assert_receive {:start_charging_process, ^car, %{latitude: +0.0}, [lookup_address: true]}
      assert_receive {:insert_charge, cproc, %{date: _, charge_energy_added: +0.0}}
      assert_receive {:"$websockex_cast", :disconnect}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :charging}}}

      assert_receive {:insert_charge, ^cproc, %{date: _, charge_energy_added: 1.0}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :charging}}}

      assert_receive {:insert_position, ^car, %{}}
      assert_receive {:insert_charge, ^cproc, %{date: _, charge_energy_added: 1.1}}
      assert_receive {:complete_charging_process, ^cproc}

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}
      refute_receive _
    end
  end

  describe "suspended" do
    test "fetches the vehicle state if the stream becomes inactive", %{test: name} do
      me = self()
      now_ts = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      events = [
        {:ok, online_event(now_ts)},
        {:ok, online_event(now_ts + 1)},
        {:ok, online_event(now_ts + 2)},
        fn ->
          send(me, :continue?)

          receive do
            :continue -> {:ok, %TeslaApi.Vehicle{state: "asleep"}}
          after
            5_000 -> raise "No :continue after 5s"
          end
        end,
        fn -> Process.sleep(10_000) end
      ]

      :ok =
        start_vehicle(name, events,
          settings: %{use_streaming_api: true, suspend_min: 999_999_999}
        )

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      assert :ok = Vehicle.suspend_logging(name)
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :suspended}}}

      send(name, {:stream, :inactive})

      assert_receive :continue?
      send(:"api_#{name}", :continue)

      assert_receive {:start_state, ^car, :asleep, [date: _]}
      assert_receive {:"$websockex_cast", :disconnect}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :asleep}}}

      refute_receive _
    end

    @tag :capture_log
    test "discards stale stream data when suspended", %{test: name} do
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)

      events = [
        {:ok, online_event(now_ts, drive_state: %{timestamp: now_ts})},
        {:ok, online_event(now_ts + 1, drive_state: %{timestamp: now_ts + 1})},
        {:ok, online_event(now_ts + 2, drive_state: %{timestamp: now_ts + 2})},
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_vehicle(name, events)

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      assert :ok = Vehicle.suspend_logging(name)
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :suspended}}}

      assert capture_log(@log_opts, fn ->
               stream(name, %{shift_state: "D", time: DateTime.add(now, -1, :millisecond)})

               # The older frame loses against the poll's drive state; only its
               # elevation, which no poll measures, is shown (#5863)
               assert_receive {:pubsub,
                               {:broadcast, _, _,
                                %Summary{state: :suspended, shift_state: nil, elevation: 10}}}

               refute_receive _
             end) =~ "Received stale stream data"
    end
  end

  test "resumes logging when starting a drive", %{test: name} do
    now = DateTime.utc_now()
    now_ts = DateTime.to_unix(now, :millisecond)

    events = [
      {:ok, online_event(now_ts)},
      {:ok, online_event(now_ts + 1)},
      {:ok, online_event(now_ts + 2)},
      fn -> Process.sleep(10_000) end
    ]

    :ok =
      start_vehicle(name, events, settings: %{suspend_min: 999_999_999})

    assert_receive {:start_state, car, :online, date: _}
    assert_receive {:insert_position, ^car, %{}}
    assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
    assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

    assert :ok = Vehicle.suspend_logging(name)
    assert_receive {:insert_position, ^car, %{}}
    assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :suspended}}}

    stream(name, %{
      shift_state: "P",
      speed: 0,
      power: 0,
      elevation: 50,
      time: DateTime.add(now, 3, :millisecond)
    })

    assert_receive {:pubsub,
                    {:broadcast, _, _,
                     %Summary{state: :suspended, shift_state: "P", elevation: 50}}}

    refute_receive _

    stream(name, %{
      shift_state: "D",
      speed: 50,
      power: 5,
      elevation: 50,
      time: DateTime.add(now, 3, :millisecond)
    })

    assert_receive {:start_drive, ^car}
    assert_receive {:insert_position, _drive, position}

    assert_receive {:pubsub,
                    {:broadcast, _, _, %Summary{state: :driving, speed: 80, elevation: 50}}}

    assert position == %{
             latitude: 42.1,
             longitude: 42.0,
             speed: 80,
             battery_level: 60,
             date: DateTime.add(now, 3, :millisecond),
             elevation: 50,
             odometer: 1609.344,
             power: 5
           }

    refute_receive _
  end

  test "resumes logging when starting to charge", %{test: name} do
    me = self()
    now = DateTime.utc_now()
    now_ts = DateTime.to_unix(now, :millisecond)

    events = [
      {:ok, online_event(now_ts, drive_state: %{timestamp: now_ts})},
      {:ok, online_event(now_ts + 1, drive_state: %{timestamp: now_ts + 1})},
      {:ok, online_event(now_ts + 2, drive_state: %{timestamp: now_ts + 2})},
      fn ->
        send(me, :continue?)

        receive do
          :continue -> {:ok, charging_event(now_ts + 3, "Charging", 0.0)}
        after
          5_000 -> raise "No :continue after 5s"
        end
      end,
      fn -> Process.sleep(10_000) end
    ]

    :ok = start_vehicle(name, events)

    assert_receive {:start_state, car, :online, date: _}
    assert_receive {:insert_position, ^car, %{}}
    assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
    assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

    assert :ok = Vehicle.suspend_logging(name)
    assert_receive {:insert_position, ^car, %{}}
    assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :suspended}}}

    stream(name, %{shift_state: nil, power: -5, time: DateTime.add(now, 2, :millisecond)})
    assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online, power: -5}}}
    assert_receive :continue?
    send(:"api_#{name}", :continue)

    assert_receive {:start_charging_process, ^car, %{latitude: +0.0}, [lookup_address: true]},
                   1000

    assert_receive {:insert_charge, _cproc, %{date: _, charge_energy_added: +0.0}}
    assert_receive {:"$websockex_cast", :disconnect}
    assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}
    assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :charging}}}

    refute_receive _
  end

  describe "updating" do
    test "disconnects stream", %{test: name} do
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)

      events = [
        {:ok, online_event(now_ts)},
        {:ok, online_event(now_ts, drive_state: %{timestamp: now_ts})},
        {:ok, update_event(now_ts + 10, "installing", "2019.8.4 530d1d3")},
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_vehicle(name, events)

      assert_receive {:start_state, car_id, :online, date: _}
      assert_receive {:insert_position, ^car_id, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      assert_receive {:start_update, ^car_id, date: _}
      assert_receive {:"$websockex_cast", :disconnect}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :updating}}}

      # A frame while updating is still an observation for the summary (#5863)
      stream(name, %{time: DateTime.utc_now()})
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :updating, elevation: 10}}}

      refute_receive _
    end
  end

  describe "summary read model (#5863)" do
    alias TeslaMate.Convert

    defp parked_event(ts, opts \\ []) do
      online_event(ts,
        drive_state:
          Map.merge(
            %{
              timestamp: ts,
              latitude: 42.0,
              longitude: 42.0,
              shift_state: "P",
              speed: nil,
              power: 0,
              heading: 90
            },
            Keyword.get(opts, :drive_state, %{})
          ),
        charge_state:
          Map.merge(%{timestamp: ts, battery_level: 70}, Keyword.get(opts, :charge_state, %{})),
        vehicle_state:
          Map.merge(
            %{timestamp: ts, odometer: 2000.0, car_version: ""},
            Keyword.get(opts, :vehicle_state, %{})
          )
      )
    end

    defp held_back(me, event) do
      fn ->
        send(me, :continue?)

        receive do
          :continue -> {:ok, event}
        after
          5_000 -> raise "No :continue after 5s"
        end
      end
    end

    defp start_parked(name, events) do
      :ok = start_vehicle(name, events)

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      :ok
    end

    test "nil is a value only for speed and gear", %{test: name} do
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)

      events = [
        {:ok, parked_event(now_ts)},
        held_back(
          self(),
          parked_event(now_ts + 3000,
            drive_state: %{power: nil, heading: nil},
            vehicle_state: %{odometer: 2000.52}
          )
        ),
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_parked(name, events)
      assert_receive :continue?

      stream(name, %{
        shift_state: "P",
        speed: 0,
        est_lat: 42.1,
        est_lng: 42.2,
        est_heading: 100,
        power: 2,
        soc: 69,
        odometer: 2000.5,
        elevation: 20,
        time: DateTime.add(now, 1, :second)
      })

      # The frame is newer than every sub-state of the poll; the odometer is
      # the frame's lower bound, one 0.1 mi grid step below its value
      odometer = Convert.miles_to_km(2000.4, 2)

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{
                         latitude: 42.1,
                         longitude: 42.2,
                         heading: 100,
                         power: 2,
                         battery_level: 69,
                         odometer: ^odometer,
                         elevation: 20,
                         speed: 0,
                         shift_state: "P"
                       }}}

      # Switched off: no speed, no gear; every other nil was not transmitted
      stream(name, %{
        shift_state: nil,
        speed: nil,
        est_lat: nil,
        est_lng: nil,
        est_heading: nil,
        power: nil,
        soc: nil,
        odometer: nil,
        elevation: nil,
        time: DateTime.add(now, 2, :second)
      })

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{
                         latitude: 42.1,
                         longitude: 42.2,
                         heading: 100,
                         power: 2,
                         battery_level: 69,
                         odometer: ^odometer,
                         elevation: 20,
                         speed: nil,
                         shift_state: nil
                       }}}

      # The position is one quantity: a single coordinate is not transmitted
      stream(name, %{
        shift_state: nil,
        speed: nil,
        est_lat: 43.0,
        est_lng: nil,
        est_heading: 100,
        power: 2,
        soc: 69,
        odometer: 2000.5,
        elevation: 20,
        time: DateTime.add(now, 2500, :millisecond)
      })

      refute_receive {:pubsub, {:broadcast, _, _, %Summary{latitude: 43.0}}}

      # A newer poll wins, but its untransmitted power and heading do not
      # override the frame's values
      send(:"api_#{name}", :continue)
      exact = Convert.miles_to_km(2000.52, 2)

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{
                         latitude: 42.0,
                         longitude: 42.0,
                         heading: 100,
                         power: 2,
                         battery_level: 70,
                         odometer: ^exact,
                         elevation: 20,
                         speed: nil,
                         shift_state: "P"
                       }}}

      refute_receive _
    end

    test "on equal timestamps the later arrival wins", %{test: name} do
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)
      ts = now_ts + 1000

      events = [
        {:ok, parked_event(now_ts)},
        held_back(self(), parked_event(ts)),
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_parked(name, events)
      assert_receive :continue?

      frame = %{
        shift_state: nil,
        speed: nil,
        est_lat: 42.1,
        est_lng: 42.2,
        soc: 69,
        odometer: 2000.5,
        time: DateTime.from_unix!(ts, :millisecond)
      }

      stream(name, frame)

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{latitude: 42.1, shift_state: nil, battery_level: 69}}}

      # The poll with the frame's timestamps arrives later and wins
      send(:"api_#{name}", :continue)

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{
                         latitude: 42.0,
                         shift_state: "P",
                         speed: nil,
                         battery_level: 70,
                         odometer: 3218.69
                       }}}

      # A frame with the same timestamp arriving after the poll wins again
      stream(name, frame)

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{latitude: 42.1, shift_state: nil, battery_level: 69}}}

      refute_receive _
    end

    test "a missing timestamp never counts as newer", %{test: name} do
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)

      without_timestamps =
        parked_event(now_ts + 2000,
          charge_state: %{timestamp: nil},
          vehicle_state: %{timestamp: nil}
        )

      without_timestamps = %TeslaApi.Vehicle{
        without_timestamps
        | drive_state: %{without_timestamps.drive_state | timestamp: nil}
      }

      events = [
        {:ok, parked_event(now_ts)},
        held_back(self(), without_timestamps),
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_parked(name, events)
      assert_receive :continue?

      stream(name, %{
        shift_state: nil,
        speed: nil,
        est_lat: 42.1,
        est_lng: 42.2,
        soc: 69,
        odometer: 2000.5,
        time: DateTime.add(now, 1, :second)
      })

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{latitude: 42.1, shift_state: nil, battery_level: 69}}}

      # The poll without timestamps arrives later and wins every sub-state
      send(:"api_#{name}", :continue)

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{
                         latitude: 42.0,
                         shift_state: "P",
                         speed: nil,
                         battery_level: 70,
                         odometer: 3218.69
                       }}}

      # A later frame wins against the missing timestamps
      stream(name, %{
        shift_state: nil,
        speed: nil,
        est_lat: 42.1,
        est_lng: 42.2,
        soc: 68,
        odometer: 2000.5,
        time: DateTime.add(now, 1, :second)
      })

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{latitude: 42.1, shift_state: nil, battery_level: 68}}}

      refute_receive _
    end

    test "after a restart the first poll is exact and not below the bound shown before",
         %{test: name} do
      now = DateTime.utc_now()
      now_ts = DateTime.to_unix(now, :millisecond)
      me = self()

      car = %TeslaMate.Log.Car{
        id: :rand.uniform(65536),
        eid: 0,
        vid: 1000,
        vin: "1000",
        model: "3",
        settings: %TeslaMate.Settings.CarSettings{
          use_streaming_api: true,
          req_not_unlocked: true
        }
      }

      events = [
        {:ok, parked_event(now_ts)},
        fn ->
          send(me, :continue?)

          receive do
            :continue -> {:error, :closed}
          after
            5_000 -> raise "No :continue after 5s"
          end
        end,
        {:ok, parked_event(now_ts + 2000, vehicle_state: %{odometer: 2000.26})},
        fn -> Process.sleep(10_000) end
      ]

      :ok = start_vehicle(name, events, car: car)

      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}
      assert_receive :continue?

      stream(name, %{odometer: 2000.3, elevation: 20, time: DateTime.add(now, 1, :second)})

      bound = Convert.miles_to_km(2000.2, 2)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{odometer: ^bound, elevation: 20}}}

      # The fetch parked before the restart answers the dead process; the
      # restarted one is served the next poll
      :ok = restart_vehicle(name, car)
      send(:"api_#{name}", :continue)

      # The summary starts empty: no frame, no bound, no elevation
      exact = Convert.miles_to_km(2000.26, 2)

      assert_receive {:pubsub,
                      {:broadcast, _, _,
                       %Summary{state: :online, odometer: ^exact, elevation: nil}}}

      assert exact >= bound
    end
  end

  describe "offline" do
    test "fetch state when stream get :vehicle_offline", %{test: name} do
      now_ts = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      events = [
        {:ok, online_event(now_ts)},
        {:ok, online_event(now_ts + 1)},
        fn -> Process.sleep(10_000) end
      ]

      :ok =
        start_vehicle(name, events,
          settings: %{use_streaming_api: true, suspend_min: 999_999_999}
        )

      assert_receive {:start_state, car, :online, date: _}
      assert_receive {:insert_position, ^car, %{}}
      assert_receive {ApiMock, {:stream, _eid, func}} when is_function(func)
      assert_receive {:pubsub, {:broadcast, _, _, %Summary{state: :online}}}

      assert capture_log(@log_opts, fn ->
               send(name, {:stream, :vehicle_offline})
               refute_receive _
             end) =~ "Stream reports vehicle as offline, fetching vehicle state ..."
    end
  end
end
