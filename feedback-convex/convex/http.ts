import { httpRouter } from "convex/server";
import { httpAction } from "./_generated/server";
import { api } from "./_generated/api";

const http = httpRouter();

// POST { "issue": "text", "timestamp": <ms> }  ->  stores a feedback row
http.route({
  path: "/feedback",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    let body: any;
    try {
      body = await request.json();
    } catch {
      return new Response(JSON.stringify({ error: "invalid json" }), {
        status: 400,
        headers: { "content-type": "application/json" },
      });
    }
    const issue = typeof body?.issue === "string" ? body.issue : "";
    const timestamp = typeof body?.timestamp === "number" ? body.timestamp : Date.now();
    if (!issue.trim()) {
      return new Response(JSON.stringify({ error: "issue is required" }), {
        status: 400,
        headers: { "content-type": "application/json" },
      });
    }
    await ctx.runMutation(api.feedback.submit, { issue, timestamp });
    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }),
});

export default http;
