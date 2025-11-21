import random
import pandas as pd 
from dataclasses import dataclass, field
from typing import List, Dict, Literal
import matplotlib.pyplot as plt 
import seaborn as sns 
import json
import os

PASTAS = [
    "logs",
    "status",
    "transferencias",
    "dashboards",
    "comparacao"
]

for pasta in PASTAS:
    os.makedirs(pasta, exist_ok=True)


CONFIGURACOES = {
    "Carga_Baixa": {
        "TIMESTEPS": 1000,
        "MIN_REQ_PER_TICK": 500,
        "MAX_REQ_PER_TICK": 1500,
        "BUFFER_SIZE": 500,
        "PROB_FALHA_NODE": 0.005,
        "PROB_FALHA_ATENDENTE": 0.002,
        "TEMPO_RECOVERY": 5,
        "MIN_AGENTS_PER_TYPE": 100
    },
    "Carga_Media": {
        "TIMESTEPS": 1000,
        "MIN_REQ_PER_TICK": 1000,
        "MAX_REQ_PER_TICK": 2000,
        "BUFFER_SIZE": 800,
        "PROB_FALHA_NODE": 0.01,
        "PROB_FALHA_ATENDENTE": 0.005,
        "TEMPO_RECOVERY": 10,
        "MIN_AGENTS_PER_TYPE": 100
    },
    "Carga_Alta": {
        "TIMESTEPS": 1000,
        "MIN_REQ_PER_TICK": 1500,
        "MAX_REQ_PER_TICK": 2500,
        "BUFFER_SIZE": 1200,
        "PROB_FALHA_NODE": 0.015,
        "PROB_FALHA_ATENDENTE": 0.008,
        "TEMPO_RECOVERY": 15,
        "MIN_AGENTS_PER_TYPE": 100
    }
}

req_id_counter = 0
agent_id_counter = 0

def gerar_req_id():
    global req_id_counter
    req_id_counter += 1
    return req_id_counter

def gerar_agent_id():
    global agent_id_counter
    agent_id_counter += 1
    return agent_id_counter


@dataclass
class Request:
    id: int
    tipo: Literal['suporte', 'vendas']
    criado_em: int
    servidor_atual: str = None
    transferencias: List[Dict] = field(default_factory=list)
    atendente_final: str = None
    finalizada: bool = False

    def transferir(self, origem, destino, motivo, tick):
        self.transferencias.append({
            'tick': tick,
            'origem': origem,
            'destino': destino,
            'motivo': motivo
        })
        self.servidor_atual = destino

    def finalizar(self, atendente_id):
        self.finalizada = True
        self.atendente_final = atendente_id


@dataclass
class Atendente:
    id: str
    tipo: Literal['suporte', 'vendas']
    servidor_origem: str
    ativo: bool = True
    reqs_atendidas: int = 0
    falhas: int = 0

    def falhar(self):
        self.ativo = False
        self.falhas += 1


class Servidor:
    def __init__(self, nome, capacidade_max, config):
        self.nome = nome
        self.capacidade_max = capacidade_max
        self.config = config
        self.ativo = True
        self.downtime_counter = 0
        self.atendentes: List[Atendente] = []
        
        self.fila_suporte: List[Request] = []
        self.fila_vendas: List[Request] = []
        
        self.total_atendidos = 0
        self.falhas_servidor = 0
        self._inicializar_atendentes()

    def _inicializar_atendentes(self):
        min_per_type = self.config["MIN_AGENTS_PER_TYPE"]
        
        for i in range(min_per_type):
            self.atendentes.append(
                Atendente(f"{self.nome}-SUP{gerar_agent_id()}", "suporte", self.nome)
            )
            self.atendentes.append(
                Atendente(f"{self.nome}-VEN{gerar_agent_id()}", "vendas", self.nome)
            )
        
        restante = self.capacidade_max - (min_per_type * 2)
        for i in range(restante):
            tipo = random.choice(['suporte', 'vendas'])
            self.atendentes.append(
                Atendente(f"{self.nome}-{tipo[0].upper()}{gerar_agent_id()}", tipo, self.nome)
            )

    def processar_fila(self, tick, log_callback, banco_reservas):
        if not self.ativo:
            return 0

        atendimentos = 0
        
        for atendente in self.atendentes:
            if not atendente.ativo:
                continue
                
            if random.random() < self.config["PROB_FALHA_ATENDENTE"]:
                atendente.falhar()
                log_callback("falha_atendente", {
                    "tick": tick,
                    "servidor": self.nome,
                    "atendente": atendente.id,
                    "tipo": atendente.tipo
                })
                
                if banco_reservas:
                    novo = banco_reservas.pop(0)
                    novo.tipo = atendente.tipo
                    novo.servidor_origem = self.nome
                    self.atendentes.append(novo)
                    log_callback("entrada_atendente", {
                        "tick": tick,
                        "servidor": self.nome,
                        "atendente_novo": novo.id,
                        "substituiu": atendente.id
                    })
                continue
        
        suporte_ativos = [a for a in self.atendentes if a.ativo and a.tipo == 'suporte']
        for atendente in suporte_ativos:
            if not self.fila_suporte:
                break
            
            req = self.fila_suporte.pop(0)
            req.finalizar(atendente.id)
            atendente.reqs_atendidas += 1
            atendimentos += 1
            
            log_callback("atendimento", {
                "tick": tick,
                "req_id": req.id,
                "tipo": req.tipo,
                "servidor": self.nome,
                "atendente": atendente.id,
                "transferencias": len(req.transferencias),
                "notificacao_cliente": f"Req {req.id} finalizada por {atendente.id}"
            })

        vendas_ativos = [a for a in self.atendentes if a.ativo and a.tipo == 'vendas']
        for atendente in vendas_ativos:
            if not self.fila_vendas:
                break
            
            req = self.fila_vendas.pop(0)
            req.finalizar(atendente.id)
            atendente.reqs_atendidas += 1
            atendimentos += 1
            
            log_callback("atendimento", {
                "tick": tick,
                "req_id": req.id,
                "tipo": req.tipo,
                "servidor": self.nome,
                "atendente": atendente.id,
                "transferencias": len(req.transferencias),
                "notificacao_cliente": f"Req {req.id} finalizada por {atendente.id}"
            })
        
        self.total_atendidos += atendimentos
        return atendimentos

    def simular_falha(self):
        if self.ativo and random.random() < self.config["PROB_FALHA_NODE"]:
            self.ativo = False
            self.downtime_counter = self.config["TEMPO_RECOVERY"]
            self.falhas_servidor += 1
            return True
        
        if not self.ativo:
            self.downtime_counter -= 1
            if self.downtime_counter <= 0:
                self.ativo = True
                return "recuperou"
        return False

    def tamanho_total_filas(self):
        return len(self.fila_suporte) + len(self.fila_vendas)

    def capturar_status(self, tick):
        return {
            'tick': tick,
            'servidor': self.nome,
            'ativo': self.ativo,
            'fila_suporte': len(self.fila_suporte),
            'fila_vendas': len(self.fila_vendas),
            'total_fila': self.tamanho_total_filas(),
            'atendentes_suporte_ativos': sum(1 for a in self.atendentes if a.ativo and a.tipo == 'suporte'),
            'atendentes_vendas_ativos': sum(1 for a in self.atendentes if a.ativo and a.tipo == 'vendas'),
            'total_atendidos': self.total_atendidos,
            'falhas': self.falhas_servidor
        }


class Supervisor:
    def __init__(self, servidores: List[Servidor], config):
        self.servidores = servidores
        self.config = config
        self.logs = []
        self.tabela_status = []
        self.tabela_transferencias = []
        
        self.banco_reservas = self._criar_banco_reservas(5000)

    def _criar_banco_reservas(self, qtd):
        reservas = []
        for i in range(qtd):
            tipo = 'suporte' if i % 2 == 0 else 'vendas'
            reservas.append(Atendente(f"RESERVA-{gerar_agent_id()}", tipo, "POOL"))
        return reservas

    def log(self, evento, dados):
        dados['evento'] = evento
        self.logs.append(dados)

    def balancear_carga(self, buffer_global: List[Request], tick):
        for req in list(buffer_global):
            servidores_disponiveis = [s for s in self.servidores if s.ativo]
            
            if not servidores_disponiveis:
                self.log("erro_critico", {"tick": tick, "msg": "Nenhum servidor disponivel!"})
                break

            if req.tipo == 'suporte':
                target = min(servidores_disponiveis, key=lambda s: len(s.fila_suporte))
                target.fila_suporte.append(req)
            else:
                target = min(servidores_disponiveis, key=lambda s: len(s.fila_vendas))
                target.fila_vendas.append(req)
            
            req.servidor_atual = target.nome
            buffer_global.remove(req)
            
            self.log("roteamento", {
                "tick": tick,
                "req_id": req.id,
                "tipo": req.tipo,
                "destino": target.nome
            })

    def verificar_saude(self, tick):
        for s in self.servidores:
            status = s.simular_falha()
            
            if status is True:
                self.log("falha_servidor", {"tick": tick, "servidor": s.nome})
                self._executar_failover(s, tick)
            
            elif status == "recuperou":
                self.log("recuperacao", {"tick": tick, "servidor": s.nome})
            
            self.tabela_status.append(s.capturar_status(tick))

    def _executar_failover(self, servidor_falho, tick):
        vizinhos = [v for v in self.servidores if v.nome != servidor_falho.nome and v.ativo]
        
        if not vizinhos:
            self.log("erro_critico", {"tick": tick, "msg": "Nenhum servidor para failover!"})
            return
        
        while servidor_falho.fila_suporte:
            req = servidor_falho.fila_suporte.pop(0)
            backup = min(vizinhos, key=lambda s: len(s.fila_suporte))
            backup.fila_suporte.append(req)
            req.transferir(servidor_falho.nome, backup.nome, "FALHA_SERVIDOR", tick)
            
            self.log("notificacao_cliente", {
                "tick": tick,
                "req_id": req.id,
                "mensagem": f"Req transferida de {servidor_falho.nome} para {backup.nome}"
            })
            
            self.tabela_transferencias.append({
                'tick': tick,
                'req_id': req.id,
                'tipo': req.tipo,
                'origem': servidor_falho.nome,
                'destino': backup.nome,
                'motivo': 'FALHA_SERVIDOR'
            })
        
        while servidor_falho.fila_vendas:
            req = servidor_falho.fila_vendas.pop(0)
            backup = min(vizinhos, key=lambda s: len(s.fila_vendas))
            backup.fila_vendas.append(req)
            req.transferir(servidor_falho.nome, backup.nome, "FALHA_SERVIDOR", tick)
            
            self.log("notificacao_cliente", {
                "tick": tick,
                "req_id": req.id,
                "mensagem": f"Req transferida de {servidor_falho.nome} para {backup.nome}"
            })
            
            self.tabela_transferencias.append({
                'tick': tick,
                'req_id': req.id,
                'tipo': req.tipo,
                'origem': servidor_falho.nome,
                'destino': backup.nome,
                'motivo': 'FALHA_SERVIDOR'
            })
        
        self.log("failover", {
            "tick": tick,
            "origem": servidor_falho.nome,
            "distribuido_entre": [v.nome for v in vizinhos]
        })


def calcular_buffer_total(buffer_entrada, servidores):
    return len(buffer_entrada) + sum(s.tamanho_total_filas() for s in servidores)


def rodar_simulacao(config_nome, config):
    global req_id_counter, agent_id_counter
    req_id_counter = 0
    agent_id_counter = 0
    
    print(f"\n{'='*70}")
    print(f"EXECUTANDO: {config_nome}")
    print(f"{'='*70}")
    
    servidores = [
        Servidor("Server-A", 500, config),
        Servidor("Server-B", 700, config),
        Servidor("Server-C", 1000, config)
    ]
    supervisor = Supervisor(servidores, config)
    buffer_entrada = []
    
    falhou = False
    tick_falha = -1

    for t in range(config["TIMESTEPS"]):
        qtd_novas = random.randint(config["MIN_REQ_PER_TICK"], config["MAX_REQ_PER_TICK"])
        for _ in range(qtd_novas):
            tipo = random.choice(['suporte', 'vendas'])
            req = Request(gerar_req_id(), tipo, t)
            buffer_entrada.append(req)
        
        supervisor.verificar_saude(t)
        
        supervisor.balancear_carga(buffer_entrada, t)

        for s in servidores:
            s.processar_fila(t, supervisor.log, supervisor.banco_reservas)

        buffer_total = calcular_buffer_total(buffer_entrada, servidores)
        if buffer_total > config["BUFFER_SIZE"]:
            print(f"!!! BUFFER OVERFLOW NO TICK {t} (Total: {buffer_total}) !!!")
            supervisor.log("falha_critica", {
                "tick": t,
                "msg": "Buffer Overflow",
                "buffer_total": buffer_total
            })
            falhou = True
            tick_falha = t
            break

        if t % 100 == 0:
            print(f"   Tick {t}: Backlog={buffer_total} | Entrada={len(buffer_entrada)}")

    print(f">>> Simulacao Finalizada: {'FALHOU' if falhou else 'SUCESSO'}")
    
    df_logs = pd.DataFrame(supervisor.logs)
    df_logs.to_csv(f"logs/logs_{config_nome}.csv", index=False)
    
    df_status = pd.DataFrame(supervisor.tabela_status)
    df_status.to_csv(f"status/tabela_status_{config_nome}.csv", index=False)
    
    df_transf = pd.DataFrame(supervisor.tabela_transferencias)
    if not df_transf.empty:
        df_transf.to_csv(f"transferencias/tabela_transferencias_{config_nome}.csv", index=False)
    
    print(f"  [OK] Logs salvos em /logs/")
    print(f"  [OK] Tabela Status salva em /status/")
    print(f"  [OK] Transferências salvas em /transferencias/")
    
    resumo = {
        'config': config_nome,
        'status': 'FALHA' if falhou else 'SUCESSO',
        'tick_falha': tick_falha if falhou else config["TIMESTEPS"],
        'total_atendimentos': sum(s.total_atendidos for s in servidores),
        'total_transferencias': len(supervisor.tabela_transferencias),
        'falhas_servidor': sum(s.falhas_servidor for s in servidores),
        'atendentes_substituidos': len([l for l in supervisor.logs if l.get('evento') == 'entrada_atendente']),
        'buffer_final': calcular_buffer_total(buffer_entrada, servidores)
    }
    
    print(f"\n  RESUMO:")
    print(f"    Total Atendimentos: {resumo['total_atendimentos']:,}")
    print(f"    Transferencias: {resumo['total_transferencias']}")
    print(f"    Falhas Servidor: {resumo['falhas_servidor']}")
    print(f"    Atendentes Substituidos: {resumo['atendentes_substituidos']}")
    
    return resumo, df_logs, df_status, servidores


def gerar_graficos(config_nome, df, df_status, servidores):
    print(f"\n  Gerando graficos para {config_nome}...")
    
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle(f'Dashboard - {config_nome}', fontsize=16, fontweight='bold')

    nomes_serv = [s.nome for s in servidores]
    atendimentos = [s.total_atendidos for s in servidores]
    cores = ['#3498db', '#2ecc71', '#e74c3c']
    
    axes[0, 0].bar(nomes_serv, atendimentos, color=cores)
    axes[0, 0].set_title('Total de Atendimentos por Servidor', fontsize=12, fontweight='bold')
    axes[0, 0].set_xlabel('Servidor')
    axes[0, 0].set_ylabel('Quantidade')
    
    for i, v in enumerate(atendimentos):
        axes[0, 0].text(i, v + max(atendimentos)*0.02, f'{v:,}', ha='center', fontweight='bold')
    
    df_falhas = df[df['evento'] == 'falha_servidor']
    if not df_falhas.empty:
        df_falhas_copy = df_falhas.copy()
        df_falhas_copy['time_bin'] = (df_falhas_copy['tick'] // 50) * 50
        falhas_tempo = df_falhas_copy.groupby(['time_bin', 'servidor']).size().reset_index(name='count')
        
        for servidor in falhas_tempo['servidor'].unique():
            data_srv = falhas_tempo[falhas_tempo['servidor'] == servidor]
            axes[0, 1].plot(data_srv['time_bin'], data_srv['count'], marker='o', label=servidor, linewidth=2)
        
        axes[0, 1].set_title('Falhas de Servidor ao Longo do Tempo', fontsize=12, fontweight='bold')
        axes[0, 1].set_xlabel('Timestep')
        axes[0, 1].set_ylabel('Numero de Falhas')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
    else:
        axes[0, 1].text(0.5, 0.5, 'Nenhuma Falha Registrada', 
                         ha='center', va='center', fontsize=14, transform=axes[0, 1].transAxes)
        axes[0, 1].set_title('Falhas de Servidor ao Longo do Tempo', fontsize=12, fontweight='bold')
    
    df_transf = df[df['evento'] == 'notificacao_cliente']
    total_atend = sum(atendimentos)
    
    if not df_transf.empty:
        labels = ['Atendimento Normal', 'Redirecionado (Falha)']
        sizes = [total_atend - len(df_transf), len(df_transf)]
        colors = ['#2ecc71', '#e74c3c']
        explode = (0, 0.1)
        
        axes[1, 0].pie(sizes, explode=explode, labels=labels, colors=colors, 
                        autopct='%1.1f%%', shadow=True, startangle=90)
        axes[1, 0].set_title(f'Impacto das Falhas (Total: {len(df_transf)})', 
                              fontsize=12, fontweight='bold')
    else:
        axes[1, 0].pie([100], labels=['Sem Redirecionamentos'], colors=['#2ecc71'], 
                        autopct='%1.0f%%', startangle=90)
        axes[1, 0].set_title('Impacto das Falhas', fontsize=12, fontweight='bold')
    
    if not df_status.empty and 'total_fila' in df_status.columns:
        for servidor in df_status['servidor'].unique():
            data_srv = df_status[df_status['servidor'] == servidor]
            axes[1, 1].plot(data_srv['tick'], data_srv['total_fila'], label=servidor, linewidth=2)
        
        axes[1, 1].set_title('Tamanho Total das Filas por Servidor', fontsize=12, fontweight='bold')
        axes[1, 1].set_xlabel('Timestep')
        axes[1, 1].set_ylabel('Requisicoes em Fila')
        axes[1, 1].legend()
        axes[1, 1].grid(True, alpha=0.3)
    else:
        axes[1, 1].text(0.5, 0.5, 'Dados de Fila Nao Disponiveis', 
                         ha='center', va='center', fontsize=14, transform=axes[1, 1].transAxes)
        axes[1, 1].set_title('Tamanho das Filas', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(f'dashboards/dashboard_{config_nome}.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"    [OK] Dashboard salvo em /dashboards/")


def gerar_comparacao(resultados):
    print(f"\n{'='*70}")
    print("COMPARACAO ENTRE CONFIGURACOES")
    print(f"{'='*70}\n")
    
    df_comp = pd.DataFrame(resultados)
    print(df_comp.to_string(index=False))
    df_comp.to_csv("comparacao/comparacao_configuracoes.csv", index=False)
    print("\n[OK] Tabela comparativa salva em /comparacao/")
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Comparacao Entre Configuracoes', fontsize=16, fontweight='bold')
    
    configs = df_comp['config']
    
    axes[0, 0].bar(configs, df_comp['total_atendimentos'], color='skyblue')
    axes[0, 0].set_title('Total de Atendimentos')
    axes[0, 0].set_ylabel('Quantidade')
    
    axes[0, 1].bar(configs, df_comp['total_transferencias'], color='coral')
    axes[0, 1].set_title('Transferencias (Failover)')
    axes[0, 1].set_ylabel('Quantidade')
    
    axes[1, 0].bar(configs, df_comp['falhas_servidor'], color='salmon')
    axes[1, 0].set_title('Falhas de Servidor')
    axes[1, 0].set_ylabel('Quantidade')
    
    axes[1, 1].bar(configs, df_comp['atendentes_substituidos'], color='lightgreen')
    axes[1, 1].set_title('Atendentes Substituidos')
    axes[1, 1].set_ylabel('Quantidade')
    
    plt.tight_layout()
    plt.savefig('comparacao/comparacao_visual.png', dpi=150)
    plt.close()
    print("[OK] Grafico comparativo salvo em /comparacao/\n")


if __name__ == "__main__":
    print("="*70)
    print("SISTEMA DE ATENDIMENTO DISTRIBUIDO - SIMULACAO COMPLETA")
    print("="*70)
    
    resultados = []
    todos_servidores = {}
    
    for nome_config, config in CONFIGURACOES.items():
        resumo, df_logs, df_status, servidores = rodar_simulacao(nome_config, config)
        todos_servidores[nome_config] = servidores
        gerar_graficos(nome_config, df_logs, df_status, servidores)
        resultados.append(resumo)
    
    gerar_comparacao(resultados)
    
    print("\n" + "="*70)
    print("SIMULACAO COMPLETA FINALIZADA!")
    print("="*70)
    print("\nPastas geradas:")
    print("  /logs")
    print("  /status")
    print("  /transferencias")
    print("  /dashboards")
    print("  /comparacao")
