package com.riksonpereira.banking.repository;

import com.riksonpereira.banking.entity.Account;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AccountRepository extends JpaRepository<Account, Long> {
}
