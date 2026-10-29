INSERT INTO devices (id, name, type, location) VALUES
  ('dev-001', 'Lobby Temp Sensor',   'temperature_sensor', 'Floor 1 - Lobby'),
  ('dev-002', 'Server Room Sensor',  'temperature_sensor', 'Floor 2 - Server Room'),
  ('dev-003', 'AHU North',           'hvac_unit',          'Roof'),
  ('dev-004', 'Meeting Room Sensor', 'temperature_sensor', 'Floor 3 - Room 301')
ON CONFLICT (id) DO NOTHING;

INSERT INTO readings (device_id, metric, value, ts) VALUES
  ('dev-001', 'temperature', 22.4, now() - interval '2 minutes'),
  ('dev-001', 'temperature', 22.6, now() - interval '1 minute'),
  ('dev-002', 'temperature', 29.1, now() - interval '1 minute')
ON CONFLICT DO NOTHING;
