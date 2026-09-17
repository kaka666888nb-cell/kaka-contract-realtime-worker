FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --registry=https://registry.npmjs.org
COPY src ./src
RUN node src/step1072-9-32-2-build-patch.mjs && node src/step1072-9-32-3-build-patch.mjs && node src/step1072-9-32-4-build-patch.mjs && node src/step1072-9-32-5-build-patch.mjs && node src/step1072-9-32-5-1-build-patch.mjs && node src/step1072-9-32-5-2-build-patch.mjs && node src/step1072-9-32-6-build-patch.mjs && node src/step1072-9-32-7-build-patch.mjs && npm run check
ENV NODE_ENV=production
ENV PORT=8080
EXPOSE 8080
CMD ["npm", "start"]
