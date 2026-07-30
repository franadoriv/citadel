// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// Local-only documentation site for developers consuming Citadel.
// No external hosting/deploy configuration lives here by design.
export default defineConfig({
	integrations: [
		starlight({
			title: 'Citadel',
			description:
				'Developer documentation for Citadel: authoritative game logic, realtime multiplayer, durable game services, and client SDKs.',
			customCss: ['./src/styles/citadel.css'],
			social: [
				{
					icon: 'github',
					label: 'Citadel on GitHub',
					href: 'https://github.com/franadoriv/citadel',
				},
				{
					icon: 'discord',
					label: 'Join the Citadel Discord community',
					href: 'https://discord.gg/79mpfygtQ',
				},
			],
			components: {
				SiteTitle: './src/components/SiteTitle.astro',
			},
			tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
			// Starlight ships dark mode and local search (Pagefind) out of the box.
			sidebar: [
				{
					label: 'Start here',
					items: [
						{ label: 'Introduction', slug: 'introduction' },
						{ label: 'Quickstart', slug: 'quickstart' },
						{ label: 'Build Knights vs Monsters', slug: 'tutorials/knights-vs-monsters' },
					],
				},
				{
					label: 'Concepts',
					items: [
						{ label: 'Game logic & server authority', slug: 'concepts/game-logic' },
						{ label: 'Player identity & sessions', slug: 'concepts/sessions' },
						{ label: 'Gateway, rooms & relay', slug: 'concepts/gateway' },
						{ label: 'Choosing a transport', slug: 'concepts/transports' },
						{ label: 'Messages & envelopes', slug: 'concepts/envelopes' },
					],
				},
				{
					label: 'Guides',
					collapsed: true,
					items: [
						{ label: 'Install a server release', slug: 'guides/install-server' },
						{ label: 'Production TLS and reverse proxy', slug: 'guides/production-tls' },
						{ label: 'Install a client SDK (Unity/Unreal/Godot)', slug: 'guides/install-client-sdk' },
						{ label: 'Connect a web client', slug: 'guides/web-client' },
						{ label: 'Connect a native client', slug: 'guides/native-client' },
						{ label: 'Manage a player session', slug: 'guides/manage-player-session' },
						{ label: 'Use the Rust SDK', slug: 'guides/rust-sdk' },
						{ label: 'Use the C ABI (FFI)', slug: 'guides/c-abi' },
						{ label: 'Engine integration (Unity/Unreal/Godot)', slug: 'guides/engines' },
						{ label: 'Export a Godot game for the web', slug: 'guides/godot-web' },
						{ label: 'Unity QUIC sample (C#)', slug: 'guides/unity-quic-sample' },
						{ label: 'Choose a database', slug: 'guides/choose-a-database' },
						{ label: 'Running on CockroachDB', slug: 'guides/cockroachdb' },
						{ label: 'Run Citadel with Docker', slug: 'guides/docker' },
						{ label: 'Use secure durable chat', slug: 'guides/secure-durable-chat' },
						{ label: 'Organize multi-file Lua and Python game logic', slug: 'guides/organize-game-server-logic' },
						{ label: 'Use shared static gameplay data', slug: 'guides/static-game-data' },
						{ label: 'Query indexed storage from game logic', slug: 'guides/storage-indexes' },
						{ label: 'Run a two-node matchmaker', slug: 'guides/distributed-matchmaker' },
					],
				},
				{
					label: 'Reference',
					collapsed: true,
					items: [
						{
							label: 'Client SDK',
							collapsed: true,
							items: [
								{ label: 'Overview', slug: 'reference/client-sdk' },
								{ label: 'Rust SDK', slug: 'reference/client-sdk/rust-sdk' },
								{ label: 'C ABI', slug: 'reference/client-sdk/c-abi' },
								{ label: 'Godot Web SDK', slug: 'reference/client-sdk/godot-web' },
								{ label: 'Authentication', slug: 'reference/client-sdk/authentication' },
								{ label: 'Rooms', slug: 'reference/client-sdk/rooms' },
								{ label: 'Matchmaker', slug: 'reference/client-sdk/matchmaker' },
								{ label: 'Parties', slug: 'reference/client-sdk/parties' },
								{ label: 'Maps', slug: 'reference/client-sdk/maps' },
								{ label: 'Transform sync', slug: 'reference/client-sdk/transform-sync' },
								{ label: 'Networked actors', slug: 'reference/client-sdk/networked-actors' },
								{ label: 'Friends', slug: 'reference/client-sdk/friends' },
								{ label: 'Player notifications', slug: 'reference/client-sdk/notifications' },
								{ label: 'Groups, leaderboards, chat & wallet', slug: 'reference/client-sdk/domain-features' },
								{
									label: 'NetworkPeer property replication',
									slug: 'reference/client-sdk/networkpeer-replication',
								},
							],
						},
						{
							label: 'Server SDK',
							collapsed: true,
							items: [
								{ label: 'Overview', slug: 'reference/server-sdk' },
								{ label: 'Lua runtime API', slug: 'reference/server-sdk/lua-runtime' },
								{ label: 'Python runtime API', slug: 'reference/server-sdk/python-runtime' },
								{ label: 'JavaScript runtime API', slug: 'reference/server-sdk/js-runtime' },
								{
									label: 'NetworkPeer server authority',
									slug: 'reference/server-sdk/networkpeer-authority',
								},
								{
									label: 'NetworkPeer schema evolution',
									slug: 'reference/server-sdk/networkpeer-schema-evolution',
								},
							],
						},
						{
							label: 'Admin Console API',
							collapsed: true,
							items: [
								{ label: 'Overview', slug: 'reference/admin-api' },
								{ label: 'Console login & roles', slug: 'reference/admin-api/console' },
								{ label: 'Chat', slug: 'reference/admin-api/chat' },
								{ label: 'Friends', slug: 'reference/admin-api/friends' },
								{ label: 'Groups', slug: 'reference/admin-api/groups' },
								{ label: 'Leaderboards', slug: 'reference/admin-api/leaderboards' },
								{ label: 'Notifications', slug: 'reference/admin-api/notifications' },
								{ label: 'Storage', slug: 'reference/admin-api/storage' },
								{ label: 'Database Explorer', slug: 'reference/admin-api/database-explorer' },
								{ label: 'Wallet', slug: 'reference/admin-api/wallet' },
								{
									label: 'Purchases & subscriptions',
									slug: 'reference/admin-api/purchases',
								},
								{ label: 'Audit log', slug: 'reference/admin-api/audit' },
							],
						},
						{
							label: 'Protocol',
							collapsed: true,
							items: [
								{ label: 'Overview', slug: 'reference/protocol' },
								{ label: 'Envelope format', slug: 'reference/protocol/envelope' },
								{
									label: 'Netcode codecs & wire foundation',
									slug: 'reference/protocol/netcode-codecs',
								},
								{
									label: 'NetworkPeer DeltaBunch',
									slug: 'reference/protocol/networkpeer-deltabunch',
								},
							],
						},
						{
							label: 'Operations',
							collapsed: true,
							items: [
								{ label: 'Overview', slug: 'reference/operations' },
								{ label: 'CLI', slug: 'reference/operations/cli' },
								{ label: 'Configuration (TOML)', slug: 'reference/operations/configuration' },
								{ label: 'Telemetry', slug: 'reference/operations/telemetry' },
								{ label: 'In-memory mode', slug: 'reference/operations/in-memory' },
								{ label: 'SQLite operations', slug: 'reference/operations/sqlite' },
								{ label: 'PostgreSQL operations', slug: 'reference/operations/postgresql' },
								{ label: 'CockroachDB operations', slug: 'reference/operations/cockroachdb' },
								{ label: 'MongoDB operations', slug: 'reference/operations/mongodb' },
								{ label: 'Local build & staging targets', slug: 'reference/operations/make-targets' },
								{ label: 'Container images', slug: 'reference/operations/container-images' },
								{ label: 'Generated API docs', slug: 'reference/operations/generated' },
							],
						},
					],
				},
				{
					label: 'Project',
					collapsed: true,
					items: [{ label: 'Changelog', slug: 'changelog' }],
				},
			],
		}),
	],
});
