create user logger with encrypted password 'log123!';
GRANT CONNECT ON DATABASE air_heater TO logger;
GRANT INSERT ON public.measurement TO logger;

SELECT usename, valuntil FROM pg_user;

ALTER USER charter VALID UNTIL 'infinity';

