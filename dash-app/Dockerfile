FROM python:3.9
WORKDIR /dash-app
# Copia los archivos de tu aplicación al contenedor
COPY . .

# Instala las dependencias de tu aplicación
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto en el que se ejecutará tu aplicación
EXPOSE 8050

# Comando para ejecutar la aplicación Dash
CMD ["python", "app.py"]
