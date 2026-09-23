/*
 * Copyright (c) 2016-2025 Martin Donath <martin.donath@squidfunk.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

import {
  Observable,
  Subject,
  asyncScheduler,
  combineLatestWith,
  debounceTime,
  defer,
  distinctUntilChanged,
  distinctUntilKeyChanged,
  endWith,
  filter,
  finalize,
  ignoreElements,
  map,
  merge,
  observeOn,
  of,
  repeat,
  scan,
  share,
  skip,
  switchMap,
  takeUntil,
  tap,
  withLatestFrom
} from "rxjs"

import { feature } from "~/_"
import {
  Viewport,
  getElementContainer,
  getElementSize,
  getElements,
  getLocation,
  getOptionalElement,
  watchElementSize
} from "~/browser"

import { Component } from "../_"
import { Header } from "../header"
import { Main } from "../main"

/* ----------------------------------------------------------------------------
 * Types
 * ------------------------------------------------------------------------- */

/**
 * Table of contents
 */
export interface TableOfContents {
  prev: HTMLAnchorElement[][]          /* Anchors (above the viewport) */
  active: HTMLAnchorElement[][]        /* Anchors (inside the viewport) */
  next: HTMLAnchorElement[][]          /* Anchors (below the viewport) */
}

/* ----------------------------------------------------------------------------
 * Helper types
 * ------------------------------------------------------------------------- */

/**
 * Watch options
 */
interface WatchOptions {
  viewport$: Observable<Viewport>      /* Viewport observable */
  header$: Observable<Header>          /* Header observable */
}

/**
 * Mount options
 */
interface MountOptions {
  viewport$: Observable<Viewport>      /* Viewport observable */
  header$: Observable<Header>          /* Header observable */
  main$: Observable<Main>              /* Main area observable */
  target$: Observable<HTMLElement>     /* Location target observable */
}

/* ----------------------------------------------------------------------------
 * Functions
 * ------------------------------------------------------------------------- */

/**
 * Watch table of contents
 *
 * This is effectively a scroll spy implementation which will account for the
 * fixed header and automatically re-calculate anchor offsets when the viewport
 * is resized. The returned observable will only emit if the table of contents
 * needs to be repainted.
 *
 * This implementation tracks an anchor element's entire path starting from its
 * level up to the top-most anchor element, e.g. `[h3, h2, h1]`. Although the
 * Material theme currently doesn't make use of this information, it enables
 * the styling of the entire hierarchy through customization.
 *
 * Note that all anchors inside the viewport are the items of the `active`
 * anchor list, so a section is highlighted as soon as its heading enters the
 * viewport and stays highlighted until it leaves it.
 *
 * @param el - Table of contents element
 * @param options - Options
 *
 * @returns Table of contents observable
 */
export function watchTableOfContents(
  el: HTMLElement, { viewport$, header$ }: WatchOptions
): Observable<TableOfContents> {
  const table = new Map<HTMLAnchorElement, HTMLElement>()

  /* Compute anchor-to-target mapping */
  const anchors = getElements<HTMLAnchorElement>(".md-nav__link", el)
  for (const anchor of anchors) {
    const id = decodeURIComponent(anchor.hash.substring(1))
    const target = getOptionalElement(`[id="${id}"]`)
    if (typeof target !== "undefined")
      table.set(anchor, target)
  }

  /* Compute necessary adjustment for header */
  const adjust$ = header$
    .pipe(
      map(({ height }) => height),
      distinctUntilChanged(),
      share()
    )

  /* Compute partition of previous, active and next anchors */
  const partition$ = watchElementSize(document.body)
    .pipe(
      distinctUntilKeyChanged("height"),

      /* Build index to map anchor paths to vertical offsets */
      switchMap(() => defer(() => {
        let path: HTMLAnchorElement[] = []
        return of([...table].reduce((index, [anchor, target]) => {
          while (path.length) {
            const last = table.get(path[path.length - 1])!
            if (last.tagName >= target.tagName) {
              path.pop()
            } else {
              break
            }
          }

          /* If the current anchor is hidden, continue with its parent */
          let offset = target.offsetTop
          while (!offset && target.parentElement) {
            target = target.parentElement
            offset = target.offsetTop
          }

          /* Fix anchor offsets in tables - see https://bit.ly/3CUFOcn */
          let parent = target.offsetParent as HTMLElement
          for (; parent; parent = parent.offsetParent as HTMLElement)
            offset += parent.offsetTop

          /* Map reversed anchor path to vertical offset */
          return index.set(
            [...path = [...path, anchor]].reverse(),
            offset
          )
        }, new Map<HTMLAnchorElement[], number>()))
      })
        .pipe(

          /* Sort index by vertical offset (see https://bit.ly/30z6QSO) */
          map(index => new Map([...index].sort(([, a], [, b]) => a - b))),
          combineLatestWith(adjust$),

          /* Re-compute partition when viewport offset changes */
          switchMap(([index, adjust]) => viewport$
            .pipe(
              scan(([prev, active, next], { offset: { y }, size }) => {
                const top = y + adjust
                const bottom = y + size.height

                /* Look forward - anchors between the top and the bottom of
                   the viewport enter the active partition, anchors below it
                   stay in the next partition. Anchors above the top of the
                   viewport always pass through the active partition, so that
                   all three partitions stay sorted by vertical offset. */
                while (next.length) {
                  const [, offset] = next[0]
                  if (offset < bottom) {
                    active = [...active, next.shift()!]
                  } else {
                    break
                  }
                }

                /* Look backward */
                while (active.length) {
                  const [, offset] = active[0]
                  if (offset < top) {
                    prev = [...prev, active.shift()!]
                  } else {
                    break
                  }
                }
                while (prev.length) {
                  const [, offset] = prev[prev.length - 1]
                  if (offset >= top) {
                    active = [prev.pop()!, ...active]
                  } else {
                    break
                  }
                }
                while (active.length) {
                  const [, offset] = active[active.length - 1]
                  if (offset >= bottom) {
                    next = [active.pop()!, ...next]
                  } else {
                    break
                  }
                }

                /* Return partition */
                return [prev, active, next]
              }, [[], [], [...index]]),
              distinctUntilChanged((a, b) => (
                a[0] === b[0] &&
                a[1] === b[1] &&
                a[2] === b[2]
              ))
            )
          )
        )
      )
    )

  /* Compute anchor paths for all partitions */
  return partition$
    .pipe(
      map(([prev, active, next]) => ({
        prev: prev.map(([path]) => path),
        active: active.map(([path]) => path),
        next: next.map(([path]) => path)
      }))
    )
}

/* ------------------------------------------------------------------------- */

/**
 * Height of the highlighted tail of the indicator bar
 */
const TAIL = 8

/**
 * Retrieve the anchors that are inside the viewport
 *
 * When no anchor is inside the viewport, the last anchor above it is returned,
 * so that the highlighted range never collapses while a section is read.
 *
 * @param state - Table of contents state
 *
 * @returns Anchors
 */
function getVisibleAnchors(
  { prev, active }: TableOfContents
): HTMLAnchorElement[][] {
  return active.length ? active : prev.slice(-1)
}

/**
 * Mount table of contents
 *
 * @param el - Table of contents element
 * @param options - Options
 *
 * @returns Table of contents component observable
 */
export function mountTableOfContents(
  el: HTMLElement, { viewport$, header$, main$, target$ }: MountOptions
): Observable<Component<TableOfContents>> {
  return defer(() => {
    const push$ = new Subject<TableOfContents>()
    const done$ = push$.pipe(ignoreElements(), endWith(true))
    push$.subscribe(state => {
      const visible = state.active
      const shown = new Set(visible.map(([anchor]) => anchor))

      /* Look backward */
      for (const [anchor] of state.prev) {
        anchor.classList.toggle("md-nav__link--passed", !shown.has(anchor))
        anchor.classList.remove("md-nav__link--active")
      }

      /* Look at anchors inside the viewport */
      for (const [anchor] of visible) {
        anchor.classList.remove("md-nav__link--passed")
        anchor.classList.add("md-nav__link--active")
      }

      /* Look forward */
      for (const [anchor] of state.next) {
        anchor.classList.remove("md-nav__link--passed")
        anchor.classList.remove("md-nav__link--active")
      }

      /* Compute the range of the indicator bar */
      const rect = el.getBoundingClientRect()
      if (visible.length) {
        const start = visible[0][0].getBoundingClientRect().top - rect.top
        const end = visible[visible.length - 1][0]
          .getBoundingClientRect().bottom - rect.top
        el.style.setProperty("--pm-toc-marker-top", `${start}px`)
        el.style.setProperty("--pm-toc-marker-height", `${end - start}px`)
      } else if (state.prev.length) {

        /* Only the content of the last anchor above the viewport is visible,
           but not its heading, so only the tail of the bar is highlighted */
        const end = state.prev[state.prev.length - 1][0]
          .getBoundingClientRect().bottom - rect.top
        el.style.setProperty("--pm-toc-marker-top", `${end - TAIL}px`)
        el.style.setProperty("--pm-toc-marker-height", `${TAIL}px`)
      } else {
        el.style.setProperty("--pm-toc-marker-height", "0px")
      }
    })

    /* Set up following, if enabled */
    if (feature("toc.follow")) {

      /* Toggle smooth scrolling only for anchor clicks */
      const smooth$ = merge(
        viewport$.pipe(debounceTime(1), map(() => undefined)),
        viewport$.pipe(debounceTime(250), map(() => "smooth" as const))
      )

      /* Bring active anchor into view */ // @todo: refactor
      push$
        .pipe(
          filter(state => getVisibleAnchors(state).length > 0),
          combineLatestWith(main$.pipe(observeOn(asyncScheduler))),
          withLatestFrom(smooth$)
        )
          .subscribe(([[state], behavior]) => {
            const [anchor] = getVisibleAnchors(state)[0]
            if (anchor.offsetHeight) {

              /* Retrieve overflowing container and scroll */
              const container = getElementContainer(anchor)
              if (typeof container !== "undefined") {
                const offset = anchor.offsetTop - container.offsetTop
                const { height } = getElementSize(container)
                container.scrollTo({
                  top: offset - height / 2,
                  behavior
                })
              }
            }
          })
    }

    /* Set up anchor tracking, if enabled */
    if (feature("navigation.tracking"))
      viewport$
        .pipe(
          takeUntil(done$),
          distinctUntilKeyChanged("offset"),
          debounceTime(250),
          skip(1),
          takeUntil(target$.pipe(skip(1))),
          repeat({ delay: 250 }),
          withLatestFrom(push$)
        )
          .subscribe(([, state]) => {
            const url = getLocation()

            /* Set hash fragment to active anchor */
            const anchor = getVisibleAnchors(state)[0]
            if (anchor && anchor.length) {
              const [active] = anchor
              const { hash } = new URL(active.href)
              if (url.hash !== hash) {
                url.hash = hash
                history.replaceState({}, "", `${url}`)
              }

            /* Reset anchor when at the top */
            } else {
              url.hash = ""
              history.replaceState({}, "", `${url}`)
            }
          })

    /* Create and return component */
    return watchTableOfContents(el, { viewport$, header$ })
      .pipe(
        tap(state => push$.next(state)),
        finalize(() => push$.complete()),
        map(state => ({ ref: el, ...state }))
      )
  })
}
