/**
 * Records each incoming request as an event in a Cloudflare Pipelines stream,
 * which a pipeline writes to an Iceberg table in R2 Data Catalog.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 */

export default {
	async fetch(request, env, ctx) {
		const url = new URL(request.url);
		const event = {
			timestamp: new Date().toISOString(),
			method: request.method,
			path: url.pathname,
			user_agent: request.headers.get("user-agent"),
			country: request.cf?.country ?? null,
		};

		// Field names must match the stream schema in stream-schema.json.
		ctx.waitUntil(env.STREAM.send([event]));

		return new Response("Hello World!");
	},
};
