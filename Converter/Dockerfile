FROM mcr.microsoft.com/dotnet/runtime:5.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["Converter/Converter.csproj", "Converter/"]
RUN dotnet restore "Converter/Converter.csproj"
COPY . .
WORKDIR "/src/Converter"
RUN dotnet build "Converter.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Converter.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Converter.dll"]
