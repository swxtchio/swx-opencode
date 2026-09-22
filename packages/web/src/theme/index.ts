import type { StarlightPlugin } from "@astrojs/starlight/types";
import { AstroError } from "astro/errors";
import { z } from "astro/zod";
import { overrideComponents } from "./lib/starlight";

const headerLinkSchema = z.object({
	name: z.string(),
	url: z.string(),
});

const configSchema = z
	.object({
		headerLinks: z.array(headerLinkSchema).optional(),
	})
	.optional();

export default function createPlugin(
	options?: ToolbeamDocsThemeUserConfig,
): StarlightPlugin {
	const parsedConfig = configSchema.safeParse(options);

	if (!parsedConfig.success) {
		throw new AstroError("The provided plugin configuration is invalid.");
	}
	return {
		name: "toolbeam-docs-theme",
		hooks: {
			"config:setup": ({ config, logger, updateConfig }) => {
				globalThis.toolbeamDocsThemeConfig = parsedConfig.data;
				// Update the Starlight config to inject your custom css
				updateConfig({
					components: overrideComponents(
						config,
						["Head", "Header", "Footer", "PageTitle"],
						logger,
					),
					pagination: false,
					customCss: [
						"@fontsource/ibm-plex-mono/400.css",
						"@fontsource/ibm-plex-mono/400-italic.css",
						"@fontsource/ibm-plex-mono/500.css",
						"@fontsource/ibm-plex-mono/600.css",
						"@fontsource/ibm-plex-mono/700.css",
						"./src/theme/styles/theme.css",
						"./src/theme/styles/tsdoc.css",
						"./src/theme/styles/markdown.css",
						"./src/theme/styles/headings.css",
						...(config.customCss ?? []),
					],
				});
			},
		},
	};
}

type ToolbeamDocsThemeUserConfig = z.infer<typeof configSchema>;
export type ToolbeamDocsThemeConfig = z.output<typeof configSchema>;
