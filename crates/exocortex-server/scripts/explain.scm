;; explain.scm — structured explanation trees (§10.3).
;; Walks a derivation chain backwards from a target edge, naming every input
;; fact. Returns a nested sexp; the harness LLM renders prose if wanted.

(define (walk edge depth)
  (let ([facts (edge-of edge)]
        [parents (parents-of edge)])
    (if (null? parents)
        (list 'leaf edge facts)
        (list 'derived
              edge
              facts
              (if (< depth 5)
                  (map (lambda (p) (walk p (+ depth 1))) parents)
                  '(...depth-bound...))))))

(define (explain-tree target)
  (walk target 0))
