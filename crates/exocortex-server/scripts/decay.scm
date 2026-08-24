;; decay.scm — unused edges lose strength over time (§10.3).
(define (decay strength age-days)
  (max 0.5 (* strength (- 1.0 (* 0.01 age-days)))))
