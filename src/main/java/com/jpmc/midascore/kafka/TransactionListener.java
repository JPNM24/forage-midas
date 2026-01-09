package com.jpmc.midascore.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.TransactionRecord;
import com.jpmc.midascore.foundation.incentive;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(UserRepository userRepository,
                               TransactionRecordRepository transactionRecordRepository,
                               RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(
        topics = "${general.kafka-topic}",
        groupId = "midas-core-group"
    )
    @Transactional
    public void listen(Transaction transaction) {

        UserRecord sender =
                userRepository.findById(transaction.getSenderId());
        UserRecord recipient =
                userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) return;
        if (sender.getBalance() < transaction.getAmount()) return;

        // 🔹 Call Incentive API
        incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                incentive.class
        );

        float incentiveAmount = incentive != null ? incentive.getAmount() : 0f;

        // 🔹 Apply balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(
                recipient.getBalance()
                + transaction.getAmount()
                + incentiveAmount
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentiveAmount
                );

        transactionRecordRepository.save(record);
    }
}
