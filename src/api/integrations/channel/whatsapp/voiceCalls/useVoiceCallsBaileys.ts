import { Logger } from '@config/logger.config';
import { ConnectionState, WAConnectionState, WASocket } from 'baileys';
import { io, Socket } from 'socket.io-client';

import { ClientToServerEvents, ServerToClientEvents } from './transport.type';

let baileys_connection_state: WAConnectionState = 'close';
const voiceLogger = new Logger('VoiceCallsBaileys');

export const useVoiceCallsBaileys = async (
  wavoip_token: string,
  baileys_sock: WASocket,
  status?: WAConnectionState,
  logger?: boolean,
) => {
  const logDebug = (value: any) => {
    if (logger) {
      voiceLogger.debug(value);
    }
  };

  const logError = (value: any) => {
    if (logger) {
      voiceLogger.error(value);
    }
  };

  baileys_connection_state = status ?? 'close';

  const socket: Socket<ServerToClientEvents, ClientToServerEvents> = io('https://devices.wavoip.com/baileys', {
    transports: ['websocket'],
    path: `/${wavoip_token}/websocket`,
  });

  socket.on('connect', () => {
    logDebug({ local: 'connect', socketId: socket.id });

    socket.emit(
      'init',
      baileys_sock.authState.creds.me,
      baileys_sock.authState.creds.account,
      baileys_connection_state,
    );
  });

  socket.on('disconnect', () => {
    logDebug({ local: 'disconnect' });
  });

  socket.on('connect_error', (error) => {
    if (socket.active) {
      logDebug({
        local: 'connect_error.retrying',
        error,
      });
    } else {
      logError({ local: 'connect_error', error });
    }
  });

  socket.on('onWhatsApp', async (jid, callback) => {
    try {
      const response: any = await baileys_sock.onWhatsApp(jid);

      callback(response);

      logDebug({ local: 'onWhatsApp.success', response, jid });
    } catch (error) {
      logError({ local: 'onWhatsApp.error', error });
    }
  });

  socket.on('profilePictureUrl', async (jid, type, timeoutMs, callback) => {
    try {
      const response = await baileys_sock.profilePictureUrl(jid, type, timeoutMs);

      callback(response);

      logDebug({ local: 'profilePictureUrl.success', response });
    } catch (error) {
      logError({ local: 'profilePictureUrl.error', error });
    }
  });

  socket.on('assertSessions', async (jids, force, callback) => {
    try {
      const response = await baileys_sock.assertSessions(jids);

      callback(response);

      logDebug({ local: 'assertSessions.success', response });
    } catch (error) {
      logError({ local: 'assertSessions.error', error });
    }
  });

  socket.on('createParticipantNodes', async (jids, message, extraAttrs, callback) => {
    try {
      const response = await baileys_sock.createParticipantNodes(jids, message, extraAttrs);

      callback(response, true);

      logDebug({ local: 'createParticipantNodes.success', response });
    } catch (error) {
      logError({ local: 'createParticipantNodes.error', error });
    }
  });

  socket.on('getUSyncDevices', async (jids, useCache, ignoreZeroDevices, callback) => {
    try {
      const response = await baileys_sock.getUSyncDevices(jids, useCache, ignoreZeroDevices);

      callback(response);

      logDebug({ local: 'getUSyncDevices.success', response });
    } catch (error) {
      logError({ local: 'getUSyncDevices.error', error });
    }
  });

  socket.on('generateMessageTag', async (callback) => {
    try {
      const response = await baileys_sock.generateMessageTag();

      callback(response);

      logDebug({ local: 'generateMessageTag.success', response });
    } catch (error) {
      logError({ local: 'generateMessageTag.error', error });
    }
  });

  socket.on('sendNode', async (stanza, callback) => {
    try {
      logDebug({ local: 'sendNode.request', stanza });
      const response = await baileys_sock.sendNode(stanza);

      callback(true);

      logDebug({ local: 'sendNode.success', response });
    } catch (error) {
      logError({ local: 'sendNode.error', error });
    }
  });

  socket.on('signalRepository:decryptMessage', async (jid, type, ciphertext, callback) => {
    try {
      const response = await baileys_sock.signalRepository.decryptMessage({
        jid: jid,
        type: type,
        ciphertext: ciphertext,
      });

      callback(response);

      logDebug({ local: 'decryptMessage.success', response });
    } catch (error) {
      logError({ local: 'decryptMessage.error', error });
    }
  });

  // we only use this connection data to inform the webphone that the device is connected and creeds account to generate e2e whatsapp key for make call packets
  baileys_sock.ev.on('connection.update', (update: Partial<ConnectionState>) => {
    const { connection } = update;

    if (connection) {
      baileys_connection_state = connection;
      socket
        .timeout(1000)
        .emit(
          'connection.update:status',
          baileys_sock.authState.creds.me,
          baileys_sock.authState.creds.account,
          connection,
        );
    }

    if (update.qr) {
      socket.timeout(1000).emit('connection.update:qr', update.qr);
    }
  });

  baileys_sock.ws.on('CB:call', (packet) => {
    logDebug({ local: 'signal.received', packet });
    socket.volatile.timeout(1000).emit('CB:call', packet);
  });

  baileys_sock.ws.on('CB:ack,class:call', (packet) => {
    logDebug({ local: 'signal.ackReceived', packet });
    socket.volatile.timeout(1000).emit('CB:ack,class:call', packet);
  });

  return socket;
};
