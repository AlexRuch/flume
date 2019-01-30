

# Flume-ng-core module v1.6.0-1.2.0

В flume-ng-core-1.6.0 были добавлены классы:
- `org.apache.flume.source.MultiportTCPSource`
- `org.apache.flume.source.MultiportUDPSource`
- `org.apache.flume.source.NetcatUDPSource`


## MultiportTCPSource

Основной функционал MultiportTCPSource схож с MultiportSyslogTCPSource, кроме того, что в MultiportTCPSource не происходит проверки сообщений на соответствие формату syslog'a и реализована возможность добавлять в сообщения информацию о том, с какого порта пришло  сообщение.

За основу был взят код MultiportSyslogTCPSource. Из него была убрана часть, отвечающая за парсинг syslog'а.

MultiportSyslogTCPSource:
```
org.apache.flume.source.MultiportSyslogTCPSource.java

Event parseEvent(ParsedBuffer parsedBuf, CharsetDecoder decoder) {
  ...
  try {
        event = syslogParser.parseMessage(msg, decoder.charset(), keepFields);
        if (parsedBuf.incomplete) {
          event.getHeaders().put(SyslogUtils.EVENT_STATUS,
              SyslogUtils.SyslogStatus.INCOMPLETE.getSyslogStatus());
        }
      } catch (IllegalArgumentException ex) {
        event = EventBuilder.withBody(msg, decoder.charset());
        event.getHeaders().put(SyslogUtils.EVENT_STATUS,
            SyslogUtils.SyslogStatus.INVALID.getSyslogStatus());
        logger.debug("Error parsing syslog event", ex);
      }
  return event;
}
```
MultiportTCPSource:
```
org.apache.flume.source.MultiportTCPSource.java

Event parseEvent(ParsedBuffer parsedBuf, CharsetDecoder decoder, int port) {
  ...
  event = EventBuilder.withBody(msg.getBytes());
  return event;
}
```
Информация о порте с которого пришло сообщение добавляется в начало сообщения и отделяется от остальной части табом. Для включения/отключения данного функционала используется параметр
```
org.apache.flume.source.SyslogSourceConfigurationConstants.java

public static final String CONFIG_PORT_PREFIX = "usePortPrefix";
```


```
org.apache.flume.source.MultiportTCPSource.java

@Override
public void configure(Context context) {
  ...
  isPortPrefix = context.getBoolean(SyslogSourceConfigurationConstants.CONFIG_PORT_PREFIX);
  ...
}
...

Event parseEvent(ParsedBuffer parsedBuf, CharsetDecoder decoder, int port) {
  String msg = null;
  try {
    ...
      if (isPortPrefix) {
        StringBuilder msgSB = new StringBuilder();
        msgSB.append(port);
        msgSB.append("\t");
        msgSB.append(tmpBuf);
        msg = msgSB.toString();
      } else {
          msg = tmpBuf;
      }
    }
    ...
```

## MultiportUDPSource

MultiportUDPSource позволяет принимать udp трафик с нескольких портов. Для этого создается количество udp-серверов равное количеству портов. Далее в многопоточном режиме данные поступают в channel.

В файле конфигураций flume задаются следующие параметры:

```
public static final String CONFIG_PORTS = "ports";
public static final String CONFIG_HOST = "host";
public static final String CONFIG_NUMPROCESSORS = "numProcessors";
public static final String CONFIG_BATCHSIZE = "batchSize";
public static final String CONFIG_READBUF_SIZE = "readBufferBytes";
```
`batchSize` позволяет задать количество сообщений, которые будут обрабатываться в отдельном потоке, который записывает сообщения в channel.


Создание UDP-серверов
```
package org.apache.flume.source.MultiportUDPSource.java

@Override
public void configure(Context context) {
    String portsStr = context.getString(
            SyslogSourceConfigurationConstants.CONFIG_PORTS);
    ...  
    for (String portStr : portsStr.split("\\s+")) {
        Integer port = Integer.parseInt(portStr);
        ports.add(port);
    }
    ...
}

@Override
public synchronized void start() {
  ...
    for (int port : ports) {
        new UDPServer(port).start();
    }
  ...
}
```
Класс `UDPServer` наследует класс `Thread` и в качестве параметра принимает порт. Внутри потока создается UDP socket server и `FixedThreadPool` с количеством потоков, заданных в `CONFIG_NUMPROCESSORS`.


```

private class UDPServer extends Thread {
  ...
  private void runServer() throws IOException {
    ...
    ExecutorService executor = Executors.newFixedThreadPool(numProcessors);
    int count = 0;
    while (true) {
        ByteBuffer buf = ByteBuffer.allocate(readBufferSize);
        buf.clear();
        serverSocket.receive(buf);
        if (count < batchSize) {
            messages[count] = buf;
            count++;
        } else {
          executor.submit(new Worker(messages));
          messages = new ByteBuffer[batchSize];
          count = 0;
        }
      }
    }
}
```

Для отправки сообщений в channel используется класс `Worker`, принимающий в качестве параметра массив `ByteBuffer`. `ButeBuffer` содержит в себе сообщение, переданное по UDP. Размер `ByteBuffer` фиксированный и необходимо достать из него только текст сообщения.
Для этого сначала необходимо узнать длинну сообщения.
```


packetLength = buff.position() - 1;
```
После чего создается новый массив байт, в который будет скопирован payload пакета.

```
message = new byte[packetLength];
System.arraycopy(buff.array(), 0, message, 0, packetLength);
```

Класс `Worker`, отправляющий сообщения в channel:
```
private class Worker implements Runnable {
    int packetLength;
    ByteBuffer[] messages;
    Worker(ByteBuffer[] messages) {
        this.messages = messages;
    }
    byte[] message;
    @Override
    public void run() {
        List<Event> eventList = new ArrayList<>();
        for (ByteBuffer buff : messages) {
            packetLength = buff.position() - 1;
            message = new byte[packetLength];
            System.arraycopy(buff.array(), 0, message, 0, packetLength);
            eventList.add(EventBuilder.withBody(message));
            counterGroup.incrementAndGet("events.success");
        }
        getChannelProcessor().processEventBatch(eventList);
    }
}
```

## NetcatUDPSource

Класс NetcatUDPSource был взят из apache-flume-1.8.0
