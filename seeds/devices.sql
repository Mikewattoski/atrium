INSERT INTO devices (id, name, type, location)
SELECT 'dev-' || lpad(i::text, 3, '0'),
       'Simulated Sensor ' || i,
       'temperature_sensor',
       'Floor ' || ((i - 1) / 20 + 1)
FROM generate_series(1, 100) AS i
ON CONFLICT (id) DO NOTHING;
