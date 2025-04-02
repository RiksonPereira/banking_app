package com.riksonpereira.banking.service.impl;

import com.riksonpereira.banking.dto.AccountDto;
import com.riksonpereira.banking.dto.TransactionDto;
import com.riksonpereira.banking.dto.TransferFundDto;
import com.riksonpereira.banking.entity.Account;
import com.riksonpereira.banking.entity.Transaction;
import com.riksonpereira.banking.exception.AccountException;
import com.riksonpereira.banking.mapper.AccountMapper;
import com.riksonpereira.banking.repository.AccountRepository;
import com.riksonpereira.banking.repository.TransactionRepository;
import com.riksonpereira.banking.service.AccountService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Service
public class AccountServiceImpl implements AccountService {

    private AccountRepository accountRepository;

    private TransactionRepository transactionRepository;
    
    // Add a map to store locks for each account ID
    private final Map<Long, ReentrantLock> accountLocks = new ConcurrentHashMap<>();

    private static final String TRANSACTION_TYPE_DEPOSIT = "DEPOSIT";
    private static final String TRANSACTION_TYPE_WITHDRAW = "WITHDRAW";
    private static final String TRANSACTION_TYPE_TRANSFER = "TRANSFER";

    public AccountServiceImpl(AccountRepository accountRepository,
                              TransactionRepository transactionRepository) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
    }
    
    // Helper method to get or create a lock for an account ID
    private ReentrantLock getAccountLock(Long accountId) {
        return accountLocks.computeIfAbsent(accountId, id -> new ReentrantLock());
    }

    @Override
    public AccountDto createAccount(AccountDto accountDto) {
        Account account = AccountMapper.mapToAccount(accountDto);
        Account savedAccount = accountRepository.save(account);
        return AccountMapper.mapToAccountDto(savedAccount);
    }

    @Override
    public AccountDto getAccountById(Long id) {

        Account account = accountRepository
                .findById(id)
                .orElseThrow(() -> new AccountException("Account does not exists"));
        return AccountMapper.mapToAccountDto(account);
    }

    @Override
    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public AccountDto deposit(Long id, double amount) {
        ReentrantLock lock = getAccountLock(id);
        lock.lock();
        try {
            Account account = accountRepository
                    .findById(id)
                    .orElseThrow(() -> new AccountException("Account does not exists"));

            double total = account.getBalance() + amount;
            account.setBalance(total);
            Account savedAccount = accountRepository.save(account);

            Transaction transaction = new Transaction();
            transaction.setAccountId(id);
            transaction.setAmount(amount);
            transaction.setTransactionType(TRANSACTION_TYPE_DEPOSIT);
            transaction.setTimestamp(LocalDateTime.now());

            transactionRepository.save(transaction);

            return AccountMapper.mapToAccountDto(savedAccount);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public AccountDto withdraw(Long id, double amount) {
        ReentrantLock lock = getAccountLock(id);
        lock.lock();
        try {
            Account account = accountRepository
                    .findById(id)
                    .orElseThrow(() -> new AccountException("Account does not exists"));

            if(account.getBalance() < amount){
                throw new AccountException("Insufficient amount");
            }

            double total = account.getBalance() - amount;
            account.setBalance(total);
            Account savedAccount = accountRepository.save(account);

            Transaction transaction = new Transaction();
            transaction.setAccountId(id);
            transaction.setAmount(amount);
            transaction.setTransactionType(TRANSACTION_TYPE_WITHDRAW);
            transaction.setTimestamp(LocalDateTime.now());

            transactionRepository.save(transaction);

            return AccountMapper.mapToAccountDto(savedAccount);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<AccountDto> getAllAccounts() {
        List<Account> accounts = accountRepository.findAll();
        return accounts.stream().map((account) -> AccountMapper.mapToAccountDto(account))
                .collect(Collectors.toList());
    }

    @Override
    public void deleteAccount(Long id) {
        ReentrantLock lock = getAccountLock(id);
        lock.lock();
        try {
            Account account = accountRepository
                    .findById(id)
                    .orElseThrow(() -> new AccountException("Account does not exists"));

            accountRepository.deleteById(id);
            // Remove the lock for this account
            accountLocks.remove(id);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public void transferFunds(TransferFundDto transferFundDto) {
        Long fromAccountId = transferFundDto.fromAccountId();
        Long toAccountId = transferFundDto.toAccountId();
        
        // Always acquire locks in a consistent order to prevent deadlocks
        ReentrantLock firstLock, secondLock;
        
        if (fromAccountId < toAccountId) {
            firstLock = getAccountLock(fromAccountId);
            secondLock = getAccountLock(toAccountId);
        } else {
            firstLock = getAccountLock(toAccountId);
            secondLock = getAccountLock(fromAccountId);
        }
        
        firstLock.lock();
        try {
            secondLock.lock();
            try {
                //Retrieve the account from which we send the amount
                Account fromAccount = accountRepository
                        .findById(transferFundDto.fromAccountId())
                        .orElseThrow(() -> new AccountException("Account does not exists"));

                //Retrieve the  account to which we send the anount
                Account toAccount = accountRepository
                        .findById(transferFundDto.toAccountId())
                        .orElseThrow(() -> new AccountException("Account does not exists"));

                //validation
                if(transferFundDto.fromAccountId() == transferFundDto.toAccountId()){
                    throw new AccountException(("Transfer not prossible to the same account"));
                }
                if(fromAccount.getBalance() < transferFundDto.amount()){
                    throw new AccountException("Inefficient balance");
                }

                //Debit the amount from fromAccount object
                fromAccount.setBalance(fromAccount.getBalance() - transferFundDto.amount());

                //Credit the amount to toAccount object
                toAccount.setBalance(toAccount.getBalance() + transferFundDto.amount());

                accountRepository.save(fromAccount);

                accountRepository.save(toAccount);

                Transaction transaction = new Transaction();
                transaction.setAccountId(transferFundDto.fromAccountId());
                transaction.setAmount(transferFundDto.amount());
                transaction.setTransactionType(TRANSACTION_TYPE_TRANSFER);
                transaction.setTimestamp(LocalDateTime.now());

                transactionRepository.save(transaction);
            } finally {
                secondLock.unlock();
            }
        } finally {
            firstLock.unlock();
        }
    }

    @Override
    public List<TransactionDto> getAccountTransactions(Long accountId) {
        List<Transaction> transactions = transactionRepository.findByAccountIdOrderByTimestampDesc(accountId);

        return transactions
                .stream()
                .map((transaction) -> convertEntityToDto(transaction))
                .collect(Collectors.toList());
    }

    public TransactionDto convertEntityToDto(Transaction transaction){
        return new TransactionDto(
                transaction.getId(),
                transaction.getAccountId(),
                transaction.getAmount(),
                transaction.getTransactionType(),
                transaction.getTimestamp()
        );
    }
}
