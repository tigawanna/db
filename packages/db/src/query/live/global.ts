/**
 * Global type definition for live query options meta.
 *
 * This interface can be extended via TypeScript module augmentation
 * to add custom properties that flow from useLiveQuery / useLiveInfiniteQuery
 * hooks down to source collection sync layers (e.g. queryFn in query collections).
 *
 * @example
 * ```typescript
 * declare module "@tanstack/db" {
 *   interface LiveQueryOptionsMeta {
 *     showCompleted?: boolean
 *     category?: string
 *   }
 * }
 * ```
 */
export interface LiveQueryOptionsMeta extends Record<string, unknown> {}
