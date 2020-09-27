# Taransay Data Forwarder
Taransay environment sensor data forwarding service.

## Dates
Naive UTC datetimes are always used for measurements. Any timezone manipulation should be
done on the remote server where the data gets sent.

## Management with Systemd

### Reload configuration
Take care: if this interrupts a routine that is currently reading the configuration values,
it could get a mix of old and new configuration values. The simplest solution to this is to
either restart instead of reloading, or try to time the reload during a period where no
data is being received (e.g. right after the last one).
