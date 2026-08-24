;; reinforce.scm — belief evolution (§10.3): evidence strengthens an edge.
;; Deterministic: same inputs -> same outputs; idempotent by R-L6.
(define (reinforce strength evidence-count)
  (min 1.0 (+ strength (* 0.05 (sqrt (max 0 (- evidence-count 1)))))))
