declare module "lang-map" {
  /**
   * Returned by calling `map()`.
   *
   * Deliberately not exported: this module uses `export = map`, and TypeScript
   * rejects an export assignment in a module that has other exports (TS2309).
   * The interface is only the return type of `map()`, referenced nowhere else,
   * so consumers still get it structurally through that signature.
   */
  interface MapReturn {
    /** All extensions keyed by language name */
    extensions: Record<string, string[]>
    /** All languages keyed by file-extension */
    languages: Record<string, string[]>
  }

  /**
   * Calling `map()` gives you the raw lookup tables:
   *
   * ```js
   * const { extensions, languages } = map();
   * ```
   */
  function map(): MapReturn

  /** Static method: get extensions for a given language */
  namespace map {
    function extensions(language: string): string[]
    /** Static method: get languages for a given extension */
    function languages(extension: string): string[]
  }

  export = map
}
