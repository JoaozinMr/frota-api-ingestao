FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Frota.Api.Ingestao.csproj ./
RUN dotnet restore
COPY . ./
RUN dotnet publish -c Release -o /out --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /out ./
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER app
# A imagem aspnet não tem curl: GET /health (liveness) via bash + /dev/tcp, exige 200.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080 && printf "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n" >&3 && head -n1 <&3 | grep -q " 200 "' || exit 1
ENTRYPOINT ["dotnet", "Frota.Api.Ingestao.dll"]
