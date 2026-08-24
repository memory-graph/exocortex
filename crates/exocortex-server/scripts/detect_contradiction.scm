;; detect_contradiction.scm — contradiction detection (§10.3).
(define (detect-contradiction kinds)
  (cond ((null? kinds) #f)
        ((equal? (car kinds) 'Contradicts) #t)
        (else (detect-contradiction (cdr kinds)))))
