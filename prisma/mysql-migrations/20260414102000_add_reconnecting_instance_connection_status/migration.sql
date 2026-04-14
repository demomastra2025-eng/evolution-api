ALTER TABLE `Instance`
    MODIFY `connectionStatus` ENUM('open', 'close', 'connecting', 'reconnecting') NOT NULL DEFAULT 'open';
